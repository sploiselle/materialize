// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to migrate from the prior vers
use std::sync::Arc;

use anyhow::Context;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{ShardId, Upper};
use mz_repr::{Datum, Diff, GlobalId, TimestampManipulation};
use mz_timely_util::order::Partitioned;

use crate::types::sources::{MzOffset, SourceData};
use crate::util::antichain::MutableOffsetAntichain;
use crate::util::remap_handle::RemapHandle;

use super::CollectionMetadata;

/// A handle to a persist shard that stores remap bindings
pub struct RemapHandleMigrator<
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    read_handle: ReadHandle<SourceData, (), T, Diff>,
    write_handle: WriteHandle<SourceData, (), T, Diff>,
    as_of: Antichain<T>,
    native_source_upper: MutableAntichain<Partitioned<PartitionId, MzOffset>>,
    compat_source_upper: MutableOffsetAntichain,
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation>
    RemapHandleMigrator<T>
{
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        read_metadata: CollectionMetadata,
        write_shard: ShardId,
        // additional information to improve logging
        id: GlobalId,
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(read_metadata.persist_location)
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (_, read_handle) = persist_client
            .open::<crate::types::sources::SourceData, (), T, Diff>(read_metadata.remap_shard)
            .await
            .context("error opening persist shard")?;

        let (write_handle, _) = persist_client
            .open::<crate::types::sources::SourceData, (), T, Diff>(write_shard)
            .await
            .context("error opening persist shard")?;

        let (since, upper) = (read_handle.since(), write_handle.upper().clone());

        let as_of = match upper.as_option() {
            Some(u) if u > &T::minimum() => Antichain::from_elem(u.step_back().unwrap()),
            // Source is terminated or new; nothing to migrate
            _ => anyhow::bail!("nothing to migrate"),
        };

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {}",
            read_metadata.remap_shard
        );

        assert!(
            as_of.elements() == [T::minimum()] || PartialOrder::less_than(&as_of, &upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        tracing::info!(
            ?since,
            ?as_of,
            ?upper,
            "RemapHandleMigrator({id}) initializing"
        );

        Ok(Self {
            read_handle,
            write_handle,
            as_of,
            native_source_upper: MutableAntichain::new(),
            compat_source_upper: MutableOffsetAntichain::new(),
        })
    }
}

/// Unpacks a binding from a Row
fn unpack_legacy_binding(data: SourceData) -> Option<(PartitionId, MzOffset)> {
    let row = data.0.expect("invalid binding");
    let mut datums = row.iter();
    let (pid, offset) = match (datums.next(), datums.next()) {
        (Some(Datum::Int32(p)), Some(Datum::UInt64(offset))) => (PartitionId::Kafka(p), offset),
        (Some(Datum::UInt64(offset)), None) => (PartitionId::None, offset),
        _ => return None,
    };

    Some((pid, MzOffset::from(offset)))
}

#[async_trait::async_trait(?Send)]
impl<T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation>
    RemapHandle for RemapHandleMigrator<T>
{
    type FromTime = Partitioned<PartitionId, MzOffset>;
    type IntoTime = T;

    async fn compare_and_append(
        &mut self,
        updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>> {
        let row_updates: Vec<_> = updates
            .into_iter()
            .map(|(partition, ts, diff)| (dbg!(SourceData::from(dbg!(partition))), ts, diff))
            .collect();

        loop {
            let updates = row_updates
                .iter()
                .map(|(data, time, diff)| ((data, ()), time, diff));
            let upper = upper.clone();
            let new_upper = new_upper.clone();
            match self
                .write_handle
                .compare_and_append(updates, upper, new_upper)
                .await
            {
                Ok(Ok(result)) => return result,
                Ok(Err(invalid_use)) => panic!("compare_and_append failed: {invalid_use}"),
                // An external error means that the operation might have suceeded or failed but we
                // don't know. In either case it is safe to retry because:
                // * If it succeeded, then on retry we'll get an `Upper(_)` error as if some other
                //   process raced us (but we actually raced ourselves). Since the operator is
                //   built to handle concurrent instances of itself this safe to do and will
                //   correctly re-sync its state. Once it resyncs we'll re-enter `mint` and notice
                //   that there are no updates to add (because we just added them and don't know
                //   it!) and the reclock operation will proceed normally.
                // * If it failed, then we'll succeed on retry and proceed normally.
                Err(external_err) => {
                    tracing::debug!(
                        "RemapHandleMigrator compare_and_append failed: {external_err}"
                    );
                    continue;
                }
            }
        }
    }

    async fn next(
        &mut self,
    ) -> Option<(
        Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        Antichain<Self::IntoTime>,
    )> {
        let snapshot = self
            .read_handle
            .snapshot_and_fetch(self.as_of.clone())
            .await
            .expect("local since is not beyond read handle's since");

        let mut updates = Vec::with_capacity(snapshot.len());

        for ((update, _), ts, diff) in snapshot.into_iter() {
            let binding = unpack_legacy_binding(update.expect("invalid row"))?;
            updates.push((binding, ts, diff))
        }

        // The following section performs the opposite transalation as the one that
        // happened during compare_and_append.
        // Vector holding the result of the translation
        let mut native_frontier_updates = vec![];

        // First, we will sort the updates by time to be able to iterate over the groups
        updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));

        // This is very subtle. An empty collection of native Antichain elements
        // represents something different than an empty collection of compat
        // OffsetAntichain elements. The former represents the empty antichain and the
        // latter the Antichain containing the minimum element. Therefore we need to
        // always synthesize a minimum timestamp element that happens once at the as_of
        // before processing any updates from the shard.
        let mut first_ts = T::minimum();
        first_ts.advance_by(self.as_of.borrow());
        native_frontier_updates.push((Partitioned::minimum(), first_ts, 1));

        // Then, we will iterate over the group of updates in time order and produce
        // the OffsetAntichain at each point, convert it to a normal Antichain, and
        // diff it with current Antichain representation.
        for (ts, updates) in &updates.into_iter().group_by(|update| update.1.clone()) {
            let prev_native_frontier = Antichain::from(self.compat_source_upper.frontier());
            native_frontier_updates.extend(
                prev_native_frontier
                    .into_iter()
                    .map(|src_ts| (src_ts, ts.clone(), -1)),
            );

            self.compat_source_upper.update_iter(
                updates
                    .into_iter()
                    .map(|(binding, _ts, diff)| (binding, diff)),
            );
            let new_native_frontier = Antichain::from(self.compat_source_upper.frontier());

            native_frontier_updates.extend(
                new_native_frontier
                    .into_iter()
                    .map(|src_ts| (src_ts, ts.clone(), 1)),
            );
        }
        // Then, consolidate the native updates and we're done
        consolidation::consolidate_updates(&mut native_frontier_updates);

        // Finally, apply the updates to our local view of the native frontier
        self.native_source_upper.update_iter(
            native_frontier_updates
                .iter()
                .map(|(src_ts, _ts, diff)| (src_ts.clone(), *diff)),
        );

        Some((native_frontier_updates, self.write_handle.upper().clone()))
    }

    async fn compact(&mut self, new_since: Antichain<Self::IntoTime>) {
        if !PartialOrder::less_equal(self.read_handle.since(), &new_since) {
            panic!(
                "ReclockFollower: `new_since` ({:?}) is not beyond \
                `self.since` ({:?}).",
                new_since,
                self.read_handle.since(),
            );
        }
        self.read_handle.maybe_downgrade_since(&new_since).await;
    }

    fn upper(&self) -> &Antichain<Self::IntoTime> {
        self.write_handle.upper()
    }

    fn since(&self) -> &Antichain<Self::IntoTime> {
        self.read_handle.since()
    }
}
