// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::meta::table_fragments::fragment::{FragmentDistributionType, FragmentType};
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{ActorMapping, Dispatcher, DispatcherType, StreamNode};

use super::graph::StreamFragmentEdge;
use super::{CreateMaterializedViewContext, FragmentManagerRef};
use crate::cluster::ParallelUnitId;
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter<S> {
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,

    /// stream graph builder, to build streaming DAG.
    stream_graph: StreamGraphBuilder<S>,

    /// id generator, used to generate actor id.
    id_gen_manager: IdGeneratorManagerRef<S>,

    /// hash mapping, used for hash dispatcher
    hash_mapping: Vec<ParallelUnitId>,

    /// local fragment id
    next_local_fragment_id: u32,
}

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    pub fn new(
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        hash_mapping: Vec<ParallelUnitId>,
    ) -> Self {
        Self {
            fragment_graph: StreamFragmentGraph::new(),
            stream_graph: StreamGraphBuilder::new(fragment_manager),
            id_gen_manager,
            hash_mapping,
            next_local_fragment_id: 0,
        }
    }

    /// Build a stream graph in two steps:
    /// (1) Break the streaming plan into fragments with their dependency.
    /// (2) Duplicate each fragment as parallel actors.
    ///
    /// Return a pair of (1) all stream actors, and (2) the actor id of sources to be forcefully
    /// round-robin scheduled.
    pub async fn generate_graph(
        mut self,
        stream_node: &StreamNode,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>> {
        // Generate fragment graph and seal
        self.generate_fragment_graph(stream_node)?;
        let len = self.fragment_graph.fragment_len();
        let offset = self
            .id_gen_manager
            .generate_interval::<{ IdCategory::Fragment }>(len as i32)
            .await? as _;
        self.fragment_graph.seal(offset, len as u32);

        // Generate actors of the streaming plan
        self.build_graph_from_fragment()?;

        let stream_graph = self.stream_graph.build(ctx)?;

        // Serialize the graph
        stream_graph
            .iter()
            .map(|(&fragment_id, actors)| {
                Ok::<_, RwError>((
                    fragment_id,
                    Fragment {
                        fragment_id: fragment_id.as_global_id(),
                        fragment_type: self
                            .fragment_graph
                            .get_fragment(fragment_id)
                            .unwrap()
                            .fragment_type as i32,
                        distribution_type: if self
                            .fragment_graph
                            .get_fragment(fragment_id)
                            .unwrap()
                            .is_singleton
                        {
                            FragmentDistributionType::Single
                        } else {
                            FragmentDistributionType::Hash
                        } as i32,
                        actors: actors.clone(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(&mut self, stream_node: &StreamNode) -> Result<()> {
        self.build_and_add_fragment(stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    fn build_and_add_fragment(&mut self, stream_node: &StreamNode) -> Result<StreamFragment> {
        let mut fragment = self.new_stream_fragment(stream_node.clone());
        let fragment = self.build_fragment(fragment, stream_node)?;
        self.fragment_graph.add_fragment(fragment.clone());
        Ok(fragment)
    }

    /// Build new fragment and link dependencies by visiting children recursively, update
    /// `is_singleton` and `fragment_type` properties for current fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &mut self,
        mut current_fragment: StreamFragment,
        stream_node: &StreamNode,
    ) -> Result<StreamFragment> {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node()? {
            Node::SourceNode(_) => current_fragment.fragment_type = FragmentType::Source,

            Node::MaterializeNode(_) => current_fragment.fragment_type = FragmentType::Sink,

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            Node::TopNNode(_) => current_fragment.is_singleton = true,

            // TODO: Force Chain to be singleton as a workaround. Remove this if parallel Chain is
            // supported
            Node::ChainNode(_) => current_fragment.is_singleton = true,

            _ => {}
        };

        // Visit plan children.
        for child_node in stream_node.get_input() {
            match child_node.get_node()? {
                // Exchange node indicates a new child fragment.
                Node::ExchangeNode(exchange_node) => {
                    let child_fragment = self.build_and_add_fragment(child_node)?;
                    self.fragment_graph.add_edge(
                        child_fragment.get_fragment_id(),
                        current_fragment.get_fragment_id(),
                        StreamFragmentEdge {
                            dispatch_strategy: exchange_node.get_strategy()?,
                            same_worker_node: false,
                        },
                    );

                    let is_simple_dispatcher =
                        exchange_node.get_strategy()?.get_type()? == DispatcherType::Simple;

                    if is_simple_dispatcher {
                        current_fragment.is_singleton = true;
                    }
                }

                // For other children, visit recursively.
                _ => self.build_fragment(current_fragment, child_node)?,
            };
        }

        Ok(())
    }

    /// Create a new stream fragment with given node with generating a fragment id.
    fn new_stream_fragment(&mut self, node: StreamNode) -> StreamFragment {
        let fragment = StreamFragment::new(
            FragmentId::LocalId(self.next_local_fragment_id),
            Arc::new(node),
        );

        self.next_local_fragment_id += 1;

        fragment
    }

    /// Generate actor id from id generator.
    async fn gen_actor_ids(&self, parallel_degree: u32) -> Result<Range<ActorId>> {
        let start_actor_id = self
            .id_gen_manager
            .generate_interval::<{ IdCategory::Actor }>(parallel_degree as i32)
            .await? as _;

        Ok(start_actor_id..start_actor_id + parallel_degree)
    }

    /// Build stream graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_graph_from_fragment(&mut self) -> Result<()> {
        let root_fragment = self.fragment_graph.get_root_fragment();
        let current_fragment_id = current_fragment.get_fragment_id();

        let parallel_degree = if current_fragment.is_singleton() {
            1
        } else {
            self.hash_mapping.iter().unique().count() as u32
        };
        let actor_ids = self.gen_actor_ids(parallel_degree).await?;

        let node = current_fragment.get_node();

        let dispatcher = if current_fragment_id == root_fragment.get_fragment_id() {
            Dispatcher {
                r#type: DispatcherType::Broadcast as i32,
                ..Default::default()
            }
        } else {
            match node.get_node()? {
                Node::ExchangeNode(exchange_node) => {
                    // TODO: support multiple dispatchers
                    let strategy = exchange_node.get_strategy()?;
                    Dispatcher {
                        r#type: strategy.r#type,
                        column_indices: strategy.column_indices.clone(),
                        ..Default::default()
                    }
                }
                _ => {
                    return Err(RwError::from(InternalError(format!(
                        "{:?} should not found.",
                        node.get_node()
                    ))));
                }
            }
        };

        for id in actor_ids.clone() {
            let mut actor_builder = StreamActorBuilder::new(id, current_fragment_id, node.clone());

            // Construct a consistent hash mapping of actors based on that of parallel units. Set
            // the new mapping into hash dispatchers.
            if dispatcher.r#type == DispatcherType::Hash as i32 {
                // Theoretically, a hash dispatcher should have `self.hash_parallel_count` as the
                // number of its downstream actors. However, since the frontend optimizer is still
                // WIP, there exists some unoptimized situation where a hash dispatcher has ONLY
                // ONE downstream actor, which makes it behave like a simple dispatcher. As an
                // expedient, we specially compute the consistent hash mapping here. The `if`
                // branch could be removed after the optimizer has been fully implemented.
                let streaming_hash_mapping = if last_fragment_actors.len() == 1 {
                    vec![last_fragment_actors[0]; self.hash_mapping.len()]
                } else {
                    let hash_parallel_units = self.hash_mapping.iter().unique().collect_vec();
                    assert_eq!(last_fragment_actors.len(), hash_parallel_units.len());
                    let parallel_unit_actor_map: HashMap<_, _> = hash_parallel_units
                        .into_iter()
                        .zip_eq(last_fragment_actors.clone().into_iter())
                        .collect();
                    self.hash_mapping
                        .iter()
                        .map(|parallel_unit_id| parallel_unit_actor_map[parallel_unit_id])
                        .collect_vec()
                };
                actor_builder.set_hash_dispatcher(
                    dispatcher.column_indices.clone(),
                    ActorMapping {
                        hash_mapping: streaming_hash_mapping,
                    },
                )
            } else {
                actor_builder.set_dispatcher(dispatcher.clone());
            }
            self.stream_graph.add_actor(actor_builder);
        }

        let current_actor_ids = actor_ids.collect_vec();
        self.stream_graph
            .add_dependency(&current_actor_ids, &last_fragment_actors);

        // Recursively generating actors on the downstream level.
        if self.fragment_graph.has_upstream(current_fragment_id) {
            for fragment_id in self
                .fragment_graph
                .get_upstream_fragments(current_fragment_id)
                .unwrap()
            {
                self.build_graph_from_fragment(
                    self.fragment_graph.get_fragment_by_id(fragment_id).unwrap(),
                    current_actor_ids.clone(),
                )?;
            }
        };

        Ok(())
    }
}
