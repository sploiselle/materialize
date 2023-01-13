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
use mz_repr::{GlobalId, TimestampManipulation};
use mz_stash::{self, TypedCollection};

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};

use super::{Controller, StorageController};

pub(super) static SHARD_FINALIZATION: TypedCollection<ShardId, ()> =
    TypedCollection::new("storage-shards-to-finalize");

pub(super) static DROPPED_SOURCES: TypedCollection<GlobalId, ()> =
    TypedCollection::new("global-ids-of-dropped-collections");

pub(super) static DROPPED_SINKS: TypedCollection<GlobalId, ()> =
    TypedCollection::new("global-ids-of-dropped-sinks");

macro_rules! register {
    ($stash_provider:expr, $wal:expr, $entries:expr) => {
        $wal.insert_without_overwrite(
            &mut $stash_provider.state.stash,
            $entries.into_iter().map(|key| (key, ())),
        )
        .await
        .expect("must be able to write to stash")
    };
}

macro_rules! check {
    ($stash_provider:expr, $wal:expr, $key:expr) => {
        $wal.peek_key_one(&mut $stash_provider.state.stash, &$key)
            .await
            .expect("must be able to connect to stash")
            .is_some()
    };
}

macro_rules! delete_where {
    ($stash_provider:expr, $wal:expr, $set:expr) => {
        $wal.delete(&mut $stash_provider.state.stash, |k, _v| $set.contains(k))
            .await
            .expect("must be able to write to stash")
    };
}

macro_rules! truncate {
    ($stash_provider:expr, $wal:expr) => {
        $wal.delete(&mut $stash_provider.state.stash, |_k, _v| true)
            .await
            .expect("must be able to write to stash")
    };
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
    /// `true` if shard is in register for shards marked for finalization.
    pub(super) async fn shards_registered_for_finalization(&mut self, shard: ShardId) -> bool {
        check!(self, SHARD_FINALIZATION, shard)
    }

    /// Register shards for finalization. This must be called if you intend to
    /// finalize shards, before you perform any work to e.g. replace one shard
    /// with another.
    ///
    /// The reasoning behind this is that we need to identify the intent to
    /// finalize a shard so we can perform the finalization on reboot if we
    /// crash and do not find the shard in use in any collection.
    pub(super) async fn register_shards_for_finalization<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = ShardId>,
    {
        register!(self, SHARD_FINALIZATION, entries)
    }

    /// Removes the shard from the finalization register.
    ///
    /// This is appropriate to do if you can guarantee that the shard has been
    /// finalized or find the shard is still in use by some collection.
    pub(super) async fn clear_from_shard_finalization_register(
        &mut self,
        shards: &BTreeSet<ShardId>,
    ) {
        delete_where!(self, SHARD_FINALIZATION, shards)
    }

    pub(super) async fn source_id_dropped(&mut self, id: GlobalId) -> bool {
        check!(self, DROPPED_SOURCES, id)
    }

    pub(super) async fn register_dropped_source_ids<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = GlobalId>,
    {
        register!(self, DROPPED_SOURCES, entries)
    }

    pub(super) async fn clear_from_dropped_source_register(&mut self, ids: &BTreeSet<GlobalId>) {
        delete_where!(self, DROPPED_SOURCES, ids)
    }

    pub(super) async fn truncate_dropped_source_register(&mut self) {
        truncate!(self, DROPPED_SOURCES)
    }

    pub(super) async fn sink_id_dropped(&mut self, id: GlobalId) -> bool {
        check!(self, DROPPED_SINKS, id)
    }

    pub(super) async fn register_dropped_sink_ids<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = GlobalId>,
    {
        register!(self, DROPPED_SINKS, entries)
    }

    pub(super) async fn clear_from_dropped_sink_register(&mut self, ids: &BTreeSet<GlobalId>) {
        delete_where!(self, DROPPED_SINKS, ids)
    }

    pub(super) async fn truncate_dropped_sink_register(&mut self) {
        truncate!(self, DROPPED_SINKS)
    }

    pub(super) async fn reconcile_command_wals(&mut self) -> Vec<GlobalId> {
        self.reconcile_shards().await;
        let dropped_sources = self.reconcile_dropped_source_register().await;
        let dropped_sinks = self.reconcile_dropped_sink_register().await;
        dropped_sources
            .into_iter()
            .chain(dropped_sinks.into_iter())
            .collect()
    }

    pub(super) async fn truncate_command_wals(&mut self) {
        // The shard reconciliation register is self-cleaning so does not need
        // to be truncated.
        self.truncate_dropped_source_register().await;
        self.truncate_dropped_sink_register().await;
    }

    async fn reconcile_dropped_source_register(&mut self) -> BTreeSet<GlobalId> {
        let dropped_sources: BTreeSet<_> = DROPPED_SOURCES
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(|(shard_id, _)| shard_id)
            .collect();

        let (live_sources, dropped_sources): (BTreeSet<GlobalId>, BTreeSet<GlobalId>) =
            dropped_sources
                .into_iter()
                .partition(|id| self.state.collections.get(id).is_some());

        // Put dropping the sources back into the sytem.
        self.drop_sources_unvalidated(live_sources.clone().into_iter().collect())
            .await;

        // Remove sources already dropped.
        self.clear_from_dropped_source_register(&dropped_sources)
            .await;

        live_sources
    }

    async fn reconcile_dropped_sink_register(&mut self) -> BTreeSet<GlobalId> {
        let dropped_sinks: BTreeSet<_> = DROPPED_SINKS
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(|(shard_id, _)| shard_id)
            .collect();

        let (live_sinks, dropped_sinks): (BTreeSet<GlobalId>, BTreeSet<GlobalId>) = dropped_sinks
            .into_iter()
            .partition(|id| self.state.collections.get(id).is_some());

        // Put dropping the sinks back into the sytem.
        self.drop_sinks_unvalidated(live_sinks.clone().into_iter().collect());

        // Remove sinks already dropped.
        self.clear_from_dropped_sink_register(&dropped_sinks).await;

        live_sinks
    }

    /// Reconcile the state of `SHARD_FINALIZATION_WAL` with
    /// `super::METADATA_COLLECTION` on boot.
    async fn reconcile_shards(&mut self) {
        // Get all shards we're aware of from stash.
        let all_shard_data: BTreeMap<_, _> = super::METADATA_COLLECTION
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(
                |(
                    id,
                    super::DurableCollectionMetadata {
                        remap_shard,
                        data_shard,
                    },
                )| { [(id, [remap_shard, data_shard])] },
            )
            .flatten()
            .collect();

        let (known_collections, missing_collections): (
            BTreeMap<GlobalId, [ShardId; 2]>,
            BTreeMap<GlobalId, [ShardId; 2]>,
        ) = all_shard_data
            .iter()
            .partition(|(id, _)| self.state.collections.get(id).is_some());

        self.register_shards_for_finalization(
            missing_collections
                .into_iter()
                .map(|(_, shards)| shards.into_iter())
                .flatten(),
        )
        .await;

        // Get all shards marked for truncation.
        let registered_shards: BTreeSet<_> = SHARD_FINALIZATION
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(|(shard_id, _)| shard_id)
            .collect();

        if registered_shards.is_empty() {
            return;
        }

        // From all shards, remove shards that belong to collections which have
        // been truncated.
        let in_use_shards: BTreeSet<_> = known_collections
            .into_iter()
            .filter_map(|(id, shards)| {
                if self.state.collections[&id].implied_capability.is_empty() {
                    None
                } else {
                    Some(shards.into_iter())
                }
            })
            .flatten()
            .collect();

        // Determine all shards that are registered that are not in use.
        let shard_id_desc_to_truncate: Vec<_> = registered_shards
            .difference(&in_use_shards)
            .cloned()
            .map(|shard_id| (shard_id, "truncating replaced shard".to_string()))
            .collect();

        self.finalize_shards(&shard_id_desc_to_truncate).await;
    }
}
