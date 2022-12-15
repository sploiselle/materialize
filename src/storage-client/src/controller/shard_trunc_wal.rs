// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracks persist shards ready to be truncated. This identifies shards we no
//! longer use, but did not have the opportunity to truncate before e.g.
//! crashing.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_client::ShardId;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::TimestampManipulation;
use mz_stash::{self, TypedCollection};

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};

use super::Controller;

pub(super) static SHARD_TRUNC_WAL: TypedCollection<ShardId, ()> =
    TypedCollection::new("storage-shards-to-truncate");

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: super::StorageController<Timestamp = T>,
{
    /// `true` if shard is in register for shards marked for truncation.
    pub(super) async fn shards_registered_for_truncation(&mut self, shard: &ShardId) -> bool {
        SHARD_TRUNC_WAL
            .peek_key_one(&mut self.state.stash, shard)
            .await
            .expect("must be able to connect to stash")
            .is_some()
    }

    /// Register shards for truncation. This must be called if you intend to
    /// truncate shards, before you perform any work to e.g. replace one shard
    /// with another.
    ///
    /// The reasoning behind this is that we need to be able to identify the
    /// intent to truncate a shard so we can perform the truncation on reboot if
    /// we crash and do not find the shard in use in any collection.
    pub(super) async fn register_shards_for_truncation<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = ShardId>,
    {
        SHARD_TRUNC_WAL
            .insert_without_overwrite(
                &mut self.state.stash,
                entries.into_iter().map(|shard_id| (shard_id, ())),
            )
            .await
            .expect("must be able to write to stash");
    }

    /// Removes the shard from the truncation register.
    ///
    /// This is appropriate to do if you can guarantee that the shard has been
    /// truncated or find the shard is still in use by some collection.
    pub(super) async fn clear_from_shard_trunc_register(&mut self, shards: &BTreeSet<ShardId>) {
        SHARD_TRUNC_WAL
            .delete(&mut self.state.stash, |k, _v| shards.contains(k))
            .await
            .expect("must be able to write to stash");
    }

    /// Reconcile the state of `SHARD_TRUNC_WAL` with
    /// `super::METADATA_COLLECTION` on boot.
    pub(super) async fn reconcile_shard_trunc_register(&mut self) {
        // Get all shards marked for truncation.
        let registered_shards: BTreeSet<_> = SHARD_TRUNC_WAL
            .peek_one(&mut self.state.stash)
            .await
            .expect("must be able to read from stash")
            .into_iter()
            .map(|(shard_id, _)| shard_id)
            .collect();

        if registered_shards.is_empty() {
            return;
        }

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

        // From all shards, remove shards that belong to collections which have
        // been truncated.
        let in_use_shards: BTreeSet<_> = all_shard_data
            .iter()
            .filter_map(|(id, shards)| {
                self.state
                    .collections
                    .get(id)
                    .map(|c| {
                        // Truncated shard, not in use.
                        if c.implied_capability.is_empty() {
                            None
                        } else {
                            Some(shards.to_vec())
                        }
                    })
                    .flatten()
            })
            .flatten()
            .collect();

        // Determine all shards that are registered that are not in use.
        let shard_id_desc_to_truncate: Vec<_> = registered_shards
            .difference(&in_use_shards)
            .cloned()
            .map(|shard_id| (shard_id, "truncating replaced shard".to_string()))
            .collect();

        self.truncate_shards(&shard_id_desc_to_truncate).await;

        // Mark all shards reapable that are no longer in use.
        self.clear_from_shard_trunc_register(
            &registered_shards
                .difference(&in_use_shards)
                .cloned()
                .collect(),
        )
        .await;
    }
}
