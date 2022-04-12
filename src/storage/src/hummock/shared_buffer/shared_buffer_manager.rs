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
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;
use std::sync::{atomic, Arc};

use itertools::Itertools;
use parking_lot::RwLock as PLRwLock;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::Result;
use tokio::task::JoinHandle;

use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::iterator::variants::*;
use crate::hummock::key::Epoch;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator, SharedBufferItem,
};
use crate::hummock::shared_buffer::shared_buffer_uploader::{
    SharedBufferUploader, SharedBufferUploaderItem, SyncItem, SyncNotify,
};
use crate::hummock::utils::range_overlap;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

#[derive(Debug)]
pub struct SharedBufferMetrics {
    pub shared_buffer_cur_size: atomic::AtomicU64,
    pub shared_buffer_threshold_size: u64,
}

impl SharedBufferMetrics {
    pub fn new(options: &StorageConfig) -> Self {
        Self {
            shared_buffer_cur_size: atomic::AtomicU64::new(0),
            shared_buffer_threshold_size: options.shared_buffer_threshold_size as u64,
        }
    }
}

/// A manager to manage reads and writes on shared buffer.
/// Shared buffer is a node level abstraction to buffer write batches across executors.
pub struct SharedBufferManager {
    /// `shared_buffer` is a collection of immutable batches grouped by (epoch, end_key)
    shared_buffer: PLRwLock<BTreeMap<u64, BTreeMap<Vec<u8>, SharedBufferBatch>>>,
    uploader_tx: tokio::sync::mpsc::UnboundedSender<SharedBufferUploaderItem>,
    sync_tx: tokio::sync::watch::Sender<SharedBufferUploaderItem>,
    uploader_handle: JoinHandle<Result<()>>,
    stats: SharedBufferMetrics,
}

impl SharedBufferManager {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        state_store_stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> Self {
        let (uploader_tx, uploader_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sync_tx, sync_rx) =
            tokio::sync::watch::channel(SharedBufferUploaderItem::Sync(SyncItem::default()));
        let stats = SharedBufferMetrics::new(options.as_ref());

        let uploader = SharedBufferUploader::new(
            options,
            local_version_manager,
            sstable_store,
            state_store_stats,
            hummock_meta_client,
            uploader_rx,
            sync_rx,
        );
        let uploader_handle = tokio::spawn(uploader.run());
        Self {
            shared_buffer: PLRwLock::new(BTreeMap::new()),
            uploader_tx,
            sync_tx,
            uploader_handle,
            stats,
        }
    }

    /// Allocates a shared buffer budget.
    async fn allocate_space(&self, batch_size: u64, _epoch: Epoch) -> HummockResult<()> {
        let mut current_size = self.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
        let threshold = self.stats.shared_buffer_threshold_size;

        // Atomically allocates space,
        // since there could be concurrent Actors write to the shared buffer.
        'retry_allocate: loop {
            // flush shared buffer if there is no enough space
            while threshold < current_size + batch_size {
                log::info!("triggered flush: threshold {}, require {}", threshold, current_size + batch_size);
                self.sync(None).await?;
                current_size = self.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
            }

            assert!(current_size + batch_size <= threshold);
            let res = self.stats.shared_buffer_cur_size.compare_exchange(
                current_size,
                current_size + batch_size,
                Ordering::SeqCst,
                Ordering::Acquire,
            );
            match res {
                Ok(_) => {
                    break; // success
                }
                Err(old_val) => {
                    current_size = old_val;
                    continue 'retry_allocate;
                }
            }
        }
        Ok(())
    }

    /// Puts a write batch into shared buffer. The batch will be synced to S3 asynchronously.
    pub async fn write_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: Epoch,
    ) -> HummockResult<u64> {
        let batch = SharedBufferBatch::new(batch, epoch);
        let size = batch.size;

        self.allocate_space(size, epoch).await?;

        let shared_buffer_size = self.stats.shared_buffer_cur_size.load(Ordering::SeqCst);
        log::info!(
            "ingested batch size: {}, shared_buff_curr_size: {}",
            size,
            shared_buffer_size
        );

        // Write the batch to shared buffer and notify the Uploader
        self.shared_buffer
            .write()
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        self.uploader_tx
            .send(SharedBufferUploaderItem::Batch(batch))
            .map_err(HummockError::shared_buffer_error)?;
        Ok(size)
    }

    /// Puts a write batch into shared buffer. The batch will won't be synced to S3 asynchronously.
    pub fn replicate_remote_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = SharedBufferBatch::new(batch, epoch);
        self.shared_buffer
            .write()
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        Ok(())
    }

    // TODO: support time-based syncing
    pub async fn sync(&self, epoch: Option<u64>) -> HummockResult<()> {
        let sync_notify = SyncNotify::new(Arc::new(tokio::sync::Notify::new()));
        self.sync_tx
            .send(SharedBufferUploaderItem::Sync(SyncItem {
                epoch,
                notifier: sync_notify.clone(),
            }))
            .unwrap();

        sync_notify.notify.notified().await;
        let res = sync_notify.result.lock();
        match res.as_ref() {
            Ok(sync_size) => {
                // Update the shared buffer size count
                let shared_buff_prev_size = self
                    .stats
                    .shared_buffer_cur_size
                    .fetch_sub(*sync_size, Ordering::SeqCst);
                log::info!(
                    "shared_buffer_prev_size {}, sync_size {}",
                    shared_buff_prev_size,
                    sync_size
                );
                assert!(shared_buff_prev_size >= *sync_size);

                Ok(())
            }
            Err(_) => Err(HummockError::shared_buffer_error(
                "Fail to sync shared buffer",
            )),
        }
    }

    /// Searches shared buffers within the `epoch_range` for the given key.
    /// Return:
    /// - None: the key doesn't exist in the shared buffer.
    /// - Some(`HummockValue`): the `HummockValue` corresponding to the key.
    pub fn get(
        &self,
        user_key: &[u8],
        epoch_range: impl RangeBounds<u64>,
    ) -> Option<HummockValue<Vec<u8>>> {
        let guard = self.shared_buffer.read();
        for (_epoch, buffers) in guard.range(epoch_range).rev() {
            for (_, m) in buffers.range(user_key.to_vec()..) {
                if m.start_user_key() > user_key {
                    continue;
                }
                match m.get(user_key) {
                    Some(v) => return Some(v),
                    None => continue,
                }
            }
        }
        None
    }

    /// Gets a collection of forward `SharedBufferBatchIterator` to iterate data of shared buffer
    /// batches within the given `key_range` and `epoch_range`
    pub fn iters<R, B>(
        &self,
        key_range: &R,
        epoch_range: impl RangeBounds<u64>,
    ) -> Vec<SharedBufferBatchIterator<FORWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.shared_buffer
            .read()
            .range(epoch_range)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.start_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), false)
                    })
                    .map(|m| m.1.iter())
            })
            .collect_vec()
    }

    /// Gets a collection of backward `SharedBufferBatchIterator` to iterate data of shared buffer
    /// batches within the given `key_range` and `epoch_range`
    pub fn reverse_iters<R, B>(
        &self,
        key_range: &R,
        epoch_range: impl RangeBounds<u64>,
    ) -> Vec<SharedBufferBatchIterator<BACKWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.shared_buffer
            .read()
            .range(epoch_range)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.end_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), true)
                    })
                    .map(|m| m.1.reverse_iter())
            })
            .collect_vec()
    }

    /// Deletes shared buffers before a given `epoch` exclusively.
    pub fn delete_before(&self, epoch: u64) {
        let mut guard = self.shared_buffer.write();
        let new = guard.split_off(&epoch);
        *guard = new;
    }

    /// This function was called while [`SharedBufferManager`] exited.
    pub async fn wait(self) -> Result<()> {
        self.uploader_handle.await.unwrap()
    }

    pub fn reset(&mut self, epoch: u64) {
        // Reset uploader item.
        self.uploader_tx
            .send(SharedBufferUploaderItem::Reset(epoch))
            .unwrap();
        // Remove items of the given epoch from shared buffer
        self.shared_buffer.write().remove(&epoch);
    }

    pub fn stats(&self) -> &SharedBufferMetrics {
        &self.stats
    }

    #[cfg(test)]
    pub fn get_shared_buffer(&self) -> BTreeMap<u64, BTreeMap<Vec<u8>, SharedBufferBatch>> {
        self.shared_buffer.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::iterator::{
        BoxedHummockIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    };
    use crate::hummock::key::{key_with_epoch, user_key};
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::SstableStore;
    use crate::object::{InMemObjectStore, ObjectStore};

    fn new_shared_buffer_manager() -> SharedBufferManager {
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let remote_dir = "/test";
        let sstable_store = Arc::new(SstableStore::new(
            obj_client,
            remote_dir.to_string(),
            Arc::new(StateStoreMetrics::unused()),
            64 << 20,
            64 << 20,
        ));
        let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
        let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        )));
        SharedBufferManager::new(
            Arc::new(default_config_for_test()),
            vm,
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            mock_hummock_meta_client,
        )
    }

    async fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer_manager: &SharedBufferManager,
    ) -> Vec<(Vec<u8>, HummockValue<Vec<u8>>)> {
        let mut shared_buffer_items = Vec::new();
        for key in put_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::put(iterator_test_value_of(*idx).into()),
            ));
            *idx += 1;
        }
        for key in delete_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::delete(),
            ));
        }
        shared_buffer_items.sort_by(|l, r| user_key(&l.0).cmp(&r.0));
        shared_buffer_manager
            .write_batch(shared_buffer_items.clone(), epoch)
            .await
            .unwrap();
        shared_buffer_items
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_get() {
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Write a batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Get and check value with epoch 0..=epoch1
        for i in 0..3 {
            assert_eq!(
                shared_buffer_manager
                    .get(keys[i].as_slice(), ..=epoch1)
                    .unwrap(),
                shared_buffer_items1[i].1
            );
        }
        assert_eq!(
            shared_buffer_manager.get(keys[3].as_slice(), ..=epoch1),
            None
        );

        // Get and check value with epoch 0..=epoch2
        assert_eq!(
            shared_buffer_manager
                .get(keys[0].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items1[0].1
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[1].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items2[0].1
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[2].as_slice(), ..=epoch2)
                .unwrap(),
            HummockValue::delete()
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[3].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items2[2].1
        );

        // Get and check value with epoch epoch2..=epoch2
        assert_eq!(
            shared_buffer_manager.get(keys[0].as_slice(), epoch2..=epoch2),
            None
        );
        for i in 0..3 {
            assert_eq!(
                shared_buffer_manager
                    .get(keys[i + 1].as_slice(), epoch2..=epoch2)
                    .unwrap(),
                shared_buffer_items2[i].1
            );
        }
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_iter() {
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Write a batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Forward iterator with 0..=epoch1
        let range = keys[0].clone()..=keys[3].clone();
        let iters = shared_buffer_manager.iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in 0..3 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());

        // Forward iterator with 0..=epoch2
        let iters = shared_buffer_manager.iters(&range, ..=epoch2);
        assert_eq!(iters.len(), 2);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[0].clone(), epoch1)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items1[0].1
        );
        merge_iterator.next().await.unwrap();
        for i in 0..2 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i + 1].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[3].clone(), epoch2)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items2[2].1
        );
        merge_iterator.next().await.unwrap();
        assert!(!merge_iterator.is_valid());

        // Forward iterator with epoch2..=epoch2
        let iters = shared_buffer_manager.iters(&range, epoch2..=epoch2);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in 0..3 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_reverse_iter() {
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Write a batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Backward iterator with 0..=epoch1
        let range = keys[3].clone()..=keys[0].clone();
        let iters = shared_buffer_manager.reverse_iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in (0..3).rev() {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());

        // Backward iterator with 0..=epoch2
        let iters = shared_buffer_manager.reverse_iters(&range, ..=epoch2);
        assert_eq!(iters.len(), 2);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[3].clone(), epoch2)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items2[2].1
        );
        merge_iterator.next().await.unwrap();

        for i in (0..2).rev() {
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i + 1].1
            );
            merge_iterator.next().await.unwrap();
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }

        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[0].clone(), epoch1)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items1[0].1
        );
        merge_iterator.next().await.unwrap();
        assert!(!merge_iterator.is_valid());

        // Backward iterator with epoch2..=epoch2
        let iters = shared_buffer_manager.reverse_iters(&range, epoch2..=epoch2);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in (0..3).rev() {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_reset() {
        let mut shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch
        let epoch = 1;
        let shared_buffer_items =
            generate_and_write_batch(&keys, &[], epoch, &mut idx, &shared_buffer_manager).await;

        // Get and check value with epoch 0..=epoch1
        for (idx, key) in keys.iter().enumerate() {
            assert_eq!(
                shared_buffer_manager.get(key.as_slice(), ..=epoch).unwrap(),
                shared_buffer_items[idx].1
            );
        }

        // Reset shared buffer. Expect all keys are gone.
        shared_buffer_manager.reset(epoch);
        for item in &shared_buffer_items {
            assert_eq!(shared_buffer_manager.get(item.0.as_slice(), ..=epoch), None);
        }

        // Generate new items overlapping with old items and check
        keys.push(format!("key_test_{:05}", 100).as_bytes().to_vec());
        let epoch = 1;
        let new_shared_buffer_items =
            generate_and_write_batch(&keys, &[], epoch, &mut idx, &shared_buffer_manager).await;
        for (idx, key) in keys.iter().enumerate() {
            assert_eq!(
                shared_buffer_manager.get(key.as_slice(), ..=epoch).unwrap(),
                new_shared_buffer_items[idx].1
            );
        }
    }
}
