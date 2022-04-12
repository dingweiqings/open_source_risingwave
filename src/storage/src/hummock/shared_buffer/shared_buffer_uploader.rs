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

use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::{Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::{HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

#[derive(Debug, Clone)]
pub struct SyncNotify {
    pub notify: Arc<tokio::sync::Notify>,
    pub result: Arc<Mutex<HummockResult<u64>>>,
}

impl SyncNotify {
    pub fn new(notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            notify,
            result: Arc::new(Mutex::new(Ok(0))),
        }
    }
}

impl Default for SyncNotify {
    fn default() -> Self {
        Self {
            notify: Arc::new(tokio::sync::Notify::default()),
            result: Arc::new(Mutex::new(Ok(0))),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SyncItem {
    /// Epoch to sync. None means syncing all epochs.
    pub(super) epoch: Option<u64>,
    /// Notifier to notify on sync finishes
    pub(super) notifier: SyncNotify,
}

#[derive(Debug, Clone)]
pub enum SharedBufferUploaderItem {
    Batch(SharedBufferBatch),
    Sync(SyncItem),
    Reset(u64),
}

pub struct SharedBufferUploader {
    /// Batches to upload grouped by epoch
    batches_to_upload: BTreeMap<u64, Vec<SharedBufferBatch>>,
    local_version_manager: Arc<LocalVersionManager>,
    options: Arc<StorageConfig>,

    /// Statistics.
    // TODO: separate `HummockStats` from `StateStoreMetrics`.
    stats: Arc<StateStoreMetrics>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_store: SstableStoreRef,

    /// For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to true
    /// in `StorageConfig`
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader_rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,

    sync_rx: tokio::sync::watch::Receiver<SharedBufferUploaderItem>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        uploader_rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
        sync_rx: tokio::sync::watch::Receiver<SharedBufferUploaderItem>,
    ) -> Self {
        Self {
            batches_to_upload: BTreeMap::new(),
            options: options.clone(),
            local_version_manager,

            stats,
            hummock_meta_client,
            sstable_store,
            write_conflict_detector: if options.write_conflict_detection_enabled {
                Some(Arc::new(ConflictDetector::new()))
            } else {
                None
            },
            uploader_rx,
            sync_rx,
        }
    }

    /// Uploads buffer batches to S3.
    async fn sync(&mut self, epoch: u64) -> HummockResult<u64> {
        if let Some(detector) = &self.write_conflict_detector {
            detector.archive_epoch(epoch);
        }

        let buffers = match self.batches_to_upload.remove(&epoch) {
            Some(m) => m,
            None => return Ok(0),
        };

        let sync_size: u64 = buffers.iter().map(|batch| batch.size).sum();

        // Compact buffers into SSTs
        let mem_compactor_ctx = CompactorContext {
            options: self.options.clone(),
            local_version_manager: self.local_version_manager.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            sstable_store: self.sstable_store.clone(),
            stats: self.stats.clone(),
            is_share_buffer_compact: true,
        };

        let tables = Compactor::compact_shared_buffer(
            Arc::new(mem_compactor_ctx),
            buffers,
            self.stats.clone(),
        )
        .await?;

        // Add all tables at once.
        let version = self
            .hummock_meta_client
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|sst| SstableInfo {
                        id: sst.id,
                        key_range: Some(risingwave_pb::hummock::KeyRange {
                            left: sst.meta.smallest_key.clone(),
                            right: sst.meta.largest_key.clone(),
                            inf: false,
                        }),
                    })
                    .collect(),
            )
            .await?;

        // Ensure the added data is available locally
        self.local_version_manager.try_set_version(version);

        Ok(sync_size)
    }

    async fn handle(&mut self, item: SharedBufferUploaderItem) -> Result<()> {
        match item {
            SharedBufferUploaderItem::Batch(m) => {
                if let Some(detector) = &self.write_conflict_detector {
                    detector.check_conflict_and_track_write_batch(&m.inner, m.epoch);
                }

                self.batches_to_upload
                    .entry(m.epoch())
                    .or_insert(Vec::new())
                    .push(m);
                Ok(())
            }
            SharedBufferUploaderItem::Sync(sync_item) => {
                let res = match sync_item.epoch {
                    Some(e) => {
                        // Sync a specific epoch
                        self.sync(e).await
                    }
                    None => {
                        // Sync all epochs
                        let epochs = self.batches_to_upload.keys().copied().collect_vec();
                        let mut res = Ok(0);
                        let mut size_total: u64 = 0;

                        for e in epochs {
                            res = self.sync(e).await;
                            if res.is_err() {
                                break;
                            }
                            size_total += res.unwrap();
                            res = Ok(size_total);
                        }
                        res
                    }
                };

                let mut sync_res = sync_item.notifier.result.lock();
                *sync_res = res;
                sync_item.notifier.notify.notify_one();
                Ok(())
            }
            SharedBufferUploaderItem::Reset(epoch) => {
                self.batches_to_upload.remove(&epoch);
                Ok(())
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            // FIXME: write_batch and sync messages may be reordered here
            tokio::select! {
                res = self.sync_rx.changed() => {
                    match res {
                        Err(e) => {
                            log::error!("shared_buffer_uploader: sync_rx.changed() Error");
                            return Err(RwError::from(ErrorCode::InternalError(e.to_string())));
                        }
                        Ok(_) => {
                            log::info!("shared_buffer_uploader: sync_rx.changed() Ok");
                            let item = (*self.sync_rx.borrow()).clone();
                            self.handle(item).await?;
                        }
                    }
                },
                msg = self.uploader_rx.recv() => {
                    if let Some(item) = msg {
                        self.handle(item).await?;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
