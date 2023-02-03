// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use differential_dataflow::lattice::Lattice;

use timely::order::TotalOrder;
use timely::progress::Timestamp;

use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{GlobalId, TimestampManipulation};

use crate::client::{StorageCommand, StorageResponse};
use crate::controller::{ProtoStorageCommand, ProtoStorageResponse};
use crate::types::sources::IngestionDescription;

use super::{
    CollectionDescription, Controller, DataSource, DurableCollectionMetadata, StorageController,
    StorageError,
};

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,

    Self: StorageController<Timestamp = T>,
{
    /// Determine the delta between `durable_metadata` and `collections` such
    /// that:
    /// - Each remap collection's data shard is its parent collection's remap
    ///   shard.
    /// - Non-ingestion-based sources do not contain a remap shard.
    ///
    /// Apply this delta using
    /// `Controller::rewrite_collection_metadata(durable_metadata, <this
    /// function's return value>)`.
    pub(super) fn remap_shard_migration(
        &mut self,
        durable_metadata: &BTreeMap<GlobalId, DurableCollectionMetadata>,
        collections: &[(GlobalId, CollectionDescription<T>)],
    ) -> Result<BTreeMap<GlobalId, DurableCollectionMetadata>, StorageError> {
        let mut state_to_update = BTreeMap::new();

        for (id, desc) in collections {
            let current_metadata = durable_metadata
                .get(id)
                .ok_or(StorageError::IdentifierMissing(*id))?;

            match &desc.data_source {
                // Iff the ingestion specifies a remap collection...
                DataSource::Ingestion(IngestionDescription {
                    remap_collection_id,
                    ..
                }) => {
                    let current_remap_shard = current_metadata
                        .remap_shard
                        .expect("ingestion-based sources have remap shards");
                    let remap_metadata = durable_metadata
                        .get(remap_collection_id)
                        .ok_or(StorageError::IdentifierMissing(*remap_collection_id))?;

                    // Retire current remap collection data shard and replace
                    // it with the data collection's remap shard. This effects
                    // the migration.
                    if remap_metadata.data_shard != current_remap_shard {
                        let mut remap_metadata = remap_metadata.clone();
                        remap_metadata.data_shard = current_remap_shard;
                        state_to_update.insert(*remap_collection_id, remap_metadata);
                    }
                }
                // All other cases, i.e. any non-ingestion data source
                _ => {
                    // If the current metadata has a remap shard for this
                    // collection, delete it.
                    if let DurableCollectionMetadata {
                        remap_shard: Some(..),
                        data_shard: _,
                    } = current_metadata
                    {
                        let mut current_metadata = current_metadata.clone();
                        current_metadata.remap_shard = None;
                        state_to_update.insert(*id, current_metadata);
                    }
                }
            };
        }

        Ok(state_to_update)
    }
}
