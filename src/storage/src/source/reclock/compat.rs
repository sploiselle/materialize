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

use differential_dataflow::lattice::Lattice;
use mz_persist_types::Codec64;
use timely::order::TotalOrder;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

use mz_expr::PartitionId;
use mz_storage_client::types::sources::MzOffset;
use mz_storage_client::util::antichain::OffsetAntichain;
use mz_timely_util::order::Partitioned;

use crate::source::reclock::{ReclockBatch, ReclockError, ReclockFollower, ReclockOperator};

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

impl<IntoTime, Clock> ReclockOperator<Partitioned<PartitionId, MzOffset>, IntoTime, Clock>
where
    IntoTime: Timestamp + Lattice + TotalOrder + Codec64,
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
        //
        // First, take the current source upper and converting it to an OffsetAntichain
        let mut current_upper = OffsetAntichain::from(self.source_upper.frontier().to_owned());
        for (pid, offset) in source_frontier.iter() {
            // Then, for each offset in the frontier that we called with try to insert it into the
            // OffsetAntichain. `maybe_insert` will only insert it if it's larger than what's
            // already there
            current_upper.maybe_insert(pid.clone(), *offset);
        }
        // Finally, convert it back to a native frontier and mint. The frontier we produce here is
        // guaranteed to be greater than or equal to the existing source upper so the mint call
        // will never panic
        self.mint(Antichain::from(current_upper).borrow()).await
    }
}
