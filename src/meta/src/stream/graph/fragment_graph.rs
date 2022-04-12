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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::stream_plan::{DispatchStrategy, StreamNode};

use crate::model::FragmentId;

/// An edge between the nodes in the fragment graph.
#[derive(Debug)]
pub struct StreamFragmentEdge {
    /// Dispatch strategy for the fragment.
    pub dispatch_strategy: DispatchStrategy,

    /// Whether the two linked nodes should be placed on the same worker node
    pub same_worker_node: bool,
}

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone, Debug)]
pub struct StreamFragment {
    /// the allocated fragment id.
    pub fragment_id: FragmentId,

    /// root stream node in this fragment.
    pub node: Arc<StreamNode>,

    /// type of this fragment.
    pub fragment_type: FragmentType,

    /// mark whether this fragment should only have one actor.
    pub is_singleton: bool,
}

impl StreamFragment {
    pub fn new(fragment_id: FragmentId, node: Arc<StreamNode>) -> Self {
        Self {
            fragment_id,
            node,
            fragment_type: FragmentType::Others,
            is_singleton: false,
        }
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph (DAG).
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<FragmentId, StreamFragment>,

    /// stores edges between fragments: upstream => downstream.
    edges: HashMap<FragmentId, HashMap<FragmentId, StreamFragmentEdge>>,

    /// whether the graph is sealed and verified
    sealed: bool,
}

impl StreamFragmentGraph {
    pub fn new() -> Self {
        Self {
            fragments: HashMap::new(),
            edges: HashMap::new(),
            sealed: false,
        }
    }

    /// Adds a fragment to the graph.
    pub fn add_fragment(&mut self, stream_fragment: StreamFragment) {
        assert!(!self.sealed);
        assert!(stream_fragment.fragment_id.is_local());
        let ret = self
            .fragments
            .insert(stream_fragment.fragment_id, stream_fragment);
        assert!(
            ret.is_none(),
            "fragment already exists: {:?}",
            stream_fragment.fragment_id
        );
    }

    /// Links upstream to downstream in the graph.
    pub fn add_edge(
        &mut self,
        upstream_id: FragmentId,
        downstream_id: FragmentId,
        edge: StreamFragmentEdge,
    ) {
        assert!(!self.sealed);
        assert!(upstream_id.is_local());
        assert!(downstream_id.is_local());
        let ret = self
            .edges
            .entry(upstream_id)
            .or_insert_with(HashMap::new)
            .insert(downstream_id, edge);
        assert!(
            ret.is_none(),
            "edge already exists: {:?}",
            (upstream_id, downstream_id, edge)
        );
    }

    pub fn get_downstream_fragment_ids(
        &self,
        fragment_id: FragmentId,
    ) -> Option<impl Iterator<Item = &'_ FragmentId> + '_> {
        assert_eq!(fragment_id.is_global(), self.sealed);
        self.edges
            .get(&fragment_id)
            .map(|x| x.iter().map(|(k, _)| k))
    }

    pub fn get_fragment(&self, fragment_id: FragmentId) -> Option<StreamFragment> {
        assert_eq!(fragment_id.is_global(), self.sealed);
        self.fragments.get(&fragment_id).cloned()
    }

    /// Convert all local ids to global ids by `local_id + offset`
    pub fn seal(&mut self, offset: u32, len: u32) {
        self.sealed = true;
        self.fragments = self
            .fragments
            .into_iter()
            .map(|(id, fragment)| {
                let id = id.to_global_id(offset, len);
                (
                    id,
                    StreamFragment {
                        fragment_id: id,
                        ..fragment
                    },
                )
            })
            .collect();

        self.edges = self
            .edges
            .into_iter()
            .map(|(upstream, links)| {
                let upstream = upstream.to_global_id(offset, len);
                let links = links
                    .into_iter()
                    .map(|(downstream, dispatcher)| {
                        let downstream = downstream.to_global_id(offset, len);
                        (downstream, dispatcher)
                    })
                    .collect();
                (upstream, links)
            })
            .collect();
    }

    /// Number of fragments
    pub fn fragment_len(&self) -> usize {
        self.fragments.len()
    }
}
