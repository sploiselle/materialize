// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reclocking compatibility code until the whole ingestion pipeline is transformed to native
//! timestamps

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use dec::OrderedDecimal;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use futures::{stream::LocalBoxStream, StreamExt};
use itertools::Itertools;
use mz_repr::adt::numeric::Numeric;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sources::{MzOffset, SourceData};
use mz_timely_util::order::{Interval, Partitioned, RangeBound};

use crate::source::antichain::{MutableOffsetAntichain, OffsetAntichain};
use crate::source::reclock::{
    ReclockBatch, ReclockError, ReclockFollower, ReclockOperator, RemapHandle,
};

#[derive(Debug)]
pub struct CompatTimestamp<T>(T);

impl From<(PartitionId, MzOffset)> for CompatTimestamp<u64> {
    fn from((pid, offset): (PartitionId, MzOffset)) -> Self {
        assert_eq!(pid, PartitionId::None);
        Self(offset.offset)
    }
}

impl From<(PartitionId, MzOffset)> for CompatTimestamp<Partitioned<u32, u64>> {
    fn from((pid, offset): (PartitionId, MzOffset)) -> Self {
        let pid = match pid {
            PartitionId::Kafka(pid) => pid as u32,
            PartitionId::None => panic!(),
        };
        Self(Partitioned::with_partition(pid, offset.offset))
    }
}

impl ReclockFollower<Partitioned<PartitionId, MzOffset>, mz_repr::Timestamp> {
    pub fn reclock_compat<'a, M>(
        &'a self,
        batch: &'a mut HashMap<PartitionId, Vec<(M, MzOffset)>>,
    ) -> Result<
        impl Iterator<Item = (M, mz_repr::Timestamp)> + 'a,
        ReclockError<Partitioned<PartitionId, MzOffset>>,
    > {
        let mut reclock_results = HashMap::with_capacity(batch.len());
        // Eagerly compute all the reclocked times to check if we need to report an error
        for (pid, updates) in batch.iter() {
            let mut pid_results = Vec::with_capacity(updates.len());
            for (_msg, offset) in updates.iter() {
                let src_ts = Partitioned::with_partition(pid.clone(), *offset);
                pid_results.push(self.reclock_time_total(&src_ts)?);
            }
            reclock_results.insert(pid.clone(), pid_results);
        }
        Ok(batch.iter_mut().flat_map(move |(pid, updates)| {
            let results = reclock_results.remove(pid).expect("created above");
            updates
                .drain(..)
                .zip(results)
                .map(|((msg, _offset), ts)| (msg, ts))
        }))
    }
}

impl<IntoTime, Clock, Handle>
    ReclockOperator<Partitioned<PartitionId, MzOffset>, IntoTime, Handle, Clock>
where
    IntoTime: Timestamp + Lattice + TotalOrder,
    Handle: RemapHandle<FromTime = Partitioned<PartitionId, MzOffset>, IntoTime = IntoTime>,
    Clock: futures::Stream<Item = (IntoTime, Antichain<IntoTime>)> + Unpin,
{
    pub async fn mint_compat(
        &mut self,
        source_frontier: &OffsetAntichain,
    ) -> ReclockBatch<Partitioned<PartitionId, MzOffset>, IntoTime> {
        // The old API didn't require that each mint request is beyond the current upper but it was
        // doing some sort of max calculation for all the partitions. This is not possible do to
        // for generic partially order timestamps so this is what this compatibility function is
        // trying to bridge.
        let mut current_upper = OffsetAntichain::from(self.source_upper.frontier().to_owned());
        for (pid, offset) in source_frontier.iter() {
            current_upper.maybe_insert(pid.clone(), *offset);
        }
        self.mint(Antichain::from(current_upper).borrow()).await
    }
}

/// A handle to a persist shard that stores remap bindings
pub struct PersistHandle {
    read_handle: ReadHandle<SourceData, (), mz_repr::Timestamp, Diff>,
    events: LocalBoxStream<'static, ListenEvent<SourceData, (), mz_repr::Timestamp, Diff>>,
    write_handle: WriteHandle<SourceData, (), mz_repr::Timestamp, Diff>,
    snapshot_produced: bool,
    upper: Antichain<mz_repr::Timestamp>,
    as_of: Antichain<mz_repr::Timestamp>,
    pending_batch: Vec<(Partitioned<PartitionId, MzOffset>, mz_repr::Timestamp, Diff)>,
    native_source_upper: MutableAntichain<Partitioned<PartitionId, MzOffset>>,
    compat_source_upper: MutableOffsetAntichain,
    minimum_produced: bool,
}

impl PersistHandle {
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        metadata: CollectionMetadata,
        as_of: Antichain<mz_repr::Timestamp>,
        // additional information to improve logging
        id: GlobalId,
        operator: &str,
        worker_id: usize,
        worker_count: usize,
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(metadata.persist_location)
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (write_handle, read_handle) = persist_client
            .open(metadata.remap_shard)
            .await
            .context("error opening persist shard")?;

        let (since, upper) = (read_handle.since(), write_handle.upper().clone());

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?}), \
            source {id}, \
            remap_shard: {}",
            metadata.remap_shard
        );

        assert!(
            as_of.elements() == [mz_repr::Timestamp::minimum()]
                || PartialOrder::less_than(&as_of, &upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        let listener = read_handle
            .clone()
            .await
            .listen(as_of.clone())
            .await
            .expect("since <= as_of asserted");

        tracing::info!(
            ?since,
            ?as_of,
            ?upper,
            "{operator}({id}) {worker_id}/{worker_count} initializing ReclockOperator"
        );

        let events = futures::stream::unfold(listener, |mut listener| async move {
            let events = futures::stream::iter(listener.next().await);
            Some((events, listener))
        })
        .flatten()
        .boxed_local();

        Ok(Self {
            read_handle,
            events,
            write_handle,
            as_of,
            snapshot_produced: false,
            upper: Antichain::from_elem(mz_repr::Timestamp::minimum()),
            pending_batch: vec![],
            native_source_upper: MutableAntichain::new(),
            compat_source_upper: MutableOffsetAntichain::new(),
            minimum_produced: false,
        })
    }
}

/// Packs a binding into a Row.
///
/// A binding of None partition is encoded as a single datum containing the offset.
///
/// A binding of a Kafka partition is encoded as the partition datum followed by the offset datum.
fn pack_binding(partition: Partitioned<PartitionId, MzOffset>) -> SourceData {
    let mut row = Row::with_capacity(2);
    let mut packer = row.packer();

    match partition.interval() {
        Interval::Range(l, u) => match (l, u) {
            (RangeBound::Bottom, RangeBound::Top) => {
                packer.push_list(&[Datum::JsonNull, Datum::JsonNull]);
            }
            (RangeBound::Bottom, RangeBound::Elem(PartitionId::Kafka(pid))) => {
                packer.push_list(&[
                    Datum::JsonNull,
                    Datum::Numeric(OrderedDecimal(Numeric::from(*pid))),
                ]);
            }
            (RangeBound::Elem(PartitionId::Kafka(pid)), RangeBound::Top) => {
                packer.push_list(&[
                    Datum::Numeric(OrderedDecimal(Numeric::from(*pid))),
                    Datum::JsonNull,
                ]);
            }
            (
                RangeBound::Elem(PartitionId::Kafka(l_pid)),
                RangeBound::Elem(PartitionId::Kafka(u_pid)),
            ) => {
                packer.push_list(&[
                    Datum::Numeric(OrderedDecimal(Numeric::from(*l_pid))),
                    Datum::Numeric(OrderedDecimal(Numeric::from(*u_pid))),
                ]);
            }
            _ => unreachable!("don't know how to handle this partition"),
        },
        Interval::Point(PartitionId::Kafka(pid)) => {
            packer.push(Datum::Numeric(OrderedDecimal(Numeric::from(*pid))))
        }
        Interval::Point(PartitionId::None) => {}
    }

    packer.push(Datum::UInt64(partition.timestamp().offset));
    SourceData(Ok(row))
}

/// Unpacks a binding from a Row
/// See documentation of [pack_binding] for the encoded format
fn unpack_binding(data: SourceData) -> Partitioned<PartitionId, MzOffset> {
    let row = data.0.expect("invalid binding");
    let mut datums = row.iter();
    match (datums.next(), datums.next()) {
        (Some(Datum::List(list)), Some(Datum::UInt64(offset))) => {
            let mut list_iter = list.iter();
            let (lower, upper) = match (list_iter.next(), list_iter.next()) {
                (Some(Datum::JsonNull), Some(Datum::JsonNull)) => (None, None),
                (Some(Datum::JsonNull), Some(Datum::Numeric(pid))) => {
                    (None, Some(PartitionId::Kafka(pid.0.try_into().unwrap())))
                }
                (Some(Datum::Numeric(pid)), Some(Datum::JsonNull)) => {
                    (Some(PartitionId::Kafka(pid.0.try_into().unwrap())), None)
                }
                (Some(Datum::Numeric(l_pid)), Some(Datum::Numeric(u_pid))) if l_pid == u_pid => {
                    return Partitioned::with_partition(
                        PartitionId::Kafka(l_pid.0.try_into().unwrap()),
                        MzOffset::from(offset),
                    )
                }
                (Some(Datum::Numeric(l_pid)), Some(Datum::Numeric(u_pid))) if l_pid == u_pid => (
                    Some(PartitionId::Kafka(l_pid.0.try_into().unwrap())),
                    Some(PartitionId::Kafka(u_pid.0.try_into().unwrap())),
                ),
                invalid_binding => {
                    panic!("invalid binding inside Datum::List {:?}", invalid_binding)
                }
            };

            Partitioned::with_range(lower, upper, MzOffset::from(offset))
        }
        (Some(Datum::Numeric(pid)), Some(Datum::UInt64(offset))) => Partitioned::with_partition(
            PartitionId::Kafka(pid.0.try_into().unwrap()),
            MzOffset::from(offset),
        ),
        (Some(Datum::UInt64(offset)), None) => {
            Partitioned::with_partition(PartitionId::None, MzOffset::from(offset))
        }
        invalid_binding => panic!("invalid binding {:?}", invalid_binding),
    }
}

#[async_trait::async_trait(?Send)]
impl RemapHandle for PersistHandle {
    type FromTime = Partitioned<PartitionId, MzOffset>;
    type IntoTime = mz_repr::Timestamp;

    async fn compare_and_append(
        &mut self,
        mut updates: Vec<(Self::FromTime, Self::IntoTime, Diff)>,
        upper: Antichain<Self::IntoTime>,
        new_upper: Antichain<Self::IntoTime>,
    ) -> Result<(), Upper<Self::IntoTime>> {
        // First, we will sort the updates by time to be able to iterate over the groups
        updates.sort_unstable_by(|a, b| a.1.cmp(&b.1));
        let mut native_frontier = self.native_source_upper.clone();

        let mut frontier_updates = vec![];

        for (ts, updates) in &updates.into_iter().group_by(|update| update.1) {
            let deltas = native_frontier.update_iter(
                updates
                    .into_iter()
                    .map(|(src_ts, _ts, diff)| (src_ts, diff)),
            );

            frontier_updates.extend(deltas.map(|(p, diff)| (p, ts.clone(), diff)));
        }

        consolidation::consolidate_updates(&mut frontier_updates);

        let row_updates: Vec<_> = frontier_updates
            .into_iter()
            .map(|(partition, ts, diff)| ((pack_binding(partition), ()), ts, diff))
            .collect();

        loop {
            let upper = upper.clone();
            let new_upper = new_upper.clone();
            match self
                .write_handle
                .compare_and_append(row_updates.iter(), upper, new_upper)
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
                    tracing::debug!("compare_and_append failed: {external_err}");
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
        if !std::mem::replace(&mut self.snapshot_produced, true) {
            for ((update, _), ts, diff) in self
                .read_handle
                .snapshot_and_fetch(self.as_of.clone())
                .await
                .expect("local since is not beyond read handle's since")
            {
                let binding = unpack_binding(update.expect("invalid row"));
                self.pending_batch.push((binding, ts, diff));
            }
        }
        while let Some(event) = self.events.next().await {
            match event {
                ListenEvent::Progress(new_upper) => {
                    // Now it's the time to peel off a batch of pending data
                    let mut native_frontier_updates = vec![];
                    self.pending_batch.retain(|(binding, ts, diff)| {
                        if !new_upper.less_equal(ts) {
                            native_frontier_updates.push((binding.clone(), *ts, *diff));
                            false
                        } else {
                            true
                        }
                    });

                    // This is very subtle. An empty collection of native Antichain elements
                    // represents something different than an empty collection of compat
                    // OffsetAntichain elements. The former represents the empty antichain and the
                    // latter the Antichain containing the minimum element. Therefore we need to
                    // always synthesize a minimum timestamp element that happens once at the as_of
                    // before processing any updates from the shard.
                    if !std::mem::replace(&mut self.minimum_produced, true) {
                        let mut first_ts = mz_repr::Timestamp::minimum();
                        first_ts.advance_by(self.as_of.borrow());
                        native_frontier_updates.push((Partitioned::minimum(), first_ts, 1));
                    }

                    native_frontier_updates.extend(
                        self.native_source_upper
                            .frontier()
                            .into_iter()
                            .map(|partition| {
                                (
                                    partition.clone(),
                                    self.upper.as_option().cloned().unwrap(),
                                    -1,
                                )
                            }),
                    );

                    // Then, consolidate the native updates and we're done
                    consolidation::consolidate_updates(&mut native_frontier_updates);

                    // Finally, apply the updates to our local view of the native frontier
                    self.native_source_upper.update_iter(
                        native_frontier_updates
                            .iter()
                            .map(|(src_ts, _ts, diff)| (src_ts.clone(), *diff)),
                    );
                    self.upper = new_upper.clone();

                    return Some((native_frontier_updates, new_upper));
                }
                ListenEvent::Updates(msgs) => {
                    for ((update, _), ts, diff) in msgs {
                        let binding = unpack_binding(update.expect("invalid row"));
                        self.pending_batch.push((binding, ts, diff));
                    }
                }
            }
        }
        None
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
