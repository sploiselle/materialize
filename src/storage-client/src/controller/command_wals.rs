// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracks persist shards ready to be finalized, i.e. remove the ability to read
//! or write them. This identifies shards we no longer use, but did not have the
//! opportunity to finalize before e.g. crashing.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{Diff, GlobalId, TimestampManipulation};
use mz_stash::{self, TypedCollection};

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{DurableCollectionMetadata, ProtoStorageCommand, ProtoStorageResponse};

use super::{Controller, StorageController};

pub(super) static SHARD_FINALIZATION: TypedCollection<ShardId, ()> =
    TypedCollection::new("storage-shards-to-finalize");

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
    /// `true` if shard is in register for shards marked for finalization.
    pub(super) async fn is_shard_registered_for_finalization(&mut self, shard: ShardId) -> bool {
        SHARD_FINALIZATION
            .peek_key_one(&mut self.state.stash, shard)
            .await
            .expect("must be able to connect to stash")
            .is_some()
    }

    /// Register shards for finalization. This must be called if you intend to
    /// finalize shards, before you perform any work to e.g. replace one shard
    /// with another.
    ///
    /// The reasoning behind this is that we need to identify the intent to
    /// finalize a shard so we can perform the finalization on reboot if we
    /// crash and do not find the shard in use in any collection.
    #[allow(dead_code)]
    pub(super) async fn register_shards_for_finalization<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = ShardId>,
    {
        SHARD_FINALIZATION
            .insert_without_overwrite(
                &mut self.state.stash,
                entries.into_iter().map(|key| (key, ())),
            )
            .await
            .expect("must be able to write to stash")
    }

    /// Removes the shard from the finalization register.
    ///
    /// This is appropriate to do if you can guarantee that the shard has been
    /// finalized or find the shard is still in use by some collection.
    pub(super) async fn clear_from_shard_finalization_register(
        &mut self,
        shards: BTreeSet<ShardId>,
    ) {
        SHARD_FINALIZATION
            .delete(&mut self.state.stash, move |k, _v| shards.contains(k))
            .await
            .expect("must be able to write to stash")
    }

    pub(super) async fn reconcile_state_inner(&mut self) {
        // Convenience method for reading from a collection in parallel.
        async fn tx_peek<'tx, K, V>(
            tx: &'tx mz_stash::Transaction<'tx>,
            typed: &TypedCollection<K, V>,
        ) -> Vec<(K, V, Diff)>
        where
            K: mz_stash::Data,
            V: mz_stash::Data,
        {
            let collection = tx
                .collection::<K, V>(typed.name())
                .await
                .expect("named collection must exist");
            tx.peek(collection).await.expect("peek succeeds")
        }

        let (metadata, mut shard_finalization) = self
            .state
            .stash
            .with_transaction(move |tx| {
                Box::pin(async move {
                    // Query all collections in parallel.
                    Ok(futures::join!(
                        tx_peek(&tx, &super::METADATA_COLLECTION),
                        tx_peek(&tx, &SHARD_FINALIZATION),
                    ))
                })
            })
            .await
            .expect("stash operation succeeds");

        if metadata.len() == self.state.collections.len() && shard_finalization.is_empty() {
            // This is the expected case--we hopefully have not leaked any
            // state.
            return;
        }

        // Transform the shard finalization collection now so we can extend it
        // with any shards we discover we need to finalize as part of
        // reconciling collection metadata.
        let mut shard_finalization: Vec<_> = shard_finalization
            .into_iter()
            .map(|(shard, _, diff)| {
                assert!(diff == 1);
                // We expect peeks to be consolidated
                (shard, "retired shard".to_string())
            })
            .collect();

        // The coordinator forgot about collections we know about.
        if metadata.len() != self.state.collections.len() {
            let addl_shards = self.reconcile_metadata_collection(metadata).await;
            // Add these shards to avoid another read from stash.
            shard_finalization.extend(
                addl_shards
                    .into_iter()
                    .map(|id| (id, "retired_shard".to_string())),
            );
        }

        // Finalize any shards; this process is idempotent so no fear in any
        // side effects.
        if !shard_finalization.is_empty() {
            self.finalize_shards(&shard_finalization).await;
        }
    }

    /// Reconciles `super::METADATA_COLLECTION` with the catalog as expressed by
    /// the coordinator during bootstrapping.
    ///
    /// Returns the ID of shard that should be finalized. This same data is also
    /// added to stash, but keeping a copy in memory lets us avoid an additional
    /// duplicative stash read.
    async fn reconcile_metadata_collection(
        &mut self,
        mut metadata: Vec<(GlobalId, DurableCollectionMetadata, i64)>,
    ) -> Vec<ShardId> {
        metadata.retain(|(id, _, _)| self.collection(*id).is_err());

        let mut shard_to_finalize = vec![];
        let mut ids_to_drop = BTreeSet::new();

        for (
            id,
            DurableCollectionMetadata {
                data_shard,
                remap_shard,
            },
            _,
        ) in metadata
        {
            ids_to_drop.insert(id);
            shard_to_finalize.push(data_shard);
            shard_to_finalize.push(remap_shard);
        }

        self.register_shards_for_finalization(shard_to_finalize.iter().cloned())
            .await;

        super::METADATA_COLLECTION
            .delete(&mut self.state.stash, move |k, _v| ids_to_drop.contains(k))
            .await
            .expect("stash operation must succeed");

        shard_to_finalize
    }
}
