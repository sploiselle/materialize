// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use itertools::Itertools;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::PartialOrder;

use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::client::controller::CollectionMetadata;
use crate::client::errors::DataflowError;
use crate::client::sources::SourceData;
use crate::storage_state::StorageState;

pub fn render<G>(
    scope: &mut G,
    src_id: GlobalId,
    metadata: CollectionMetadata,
    source_data: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &mut StorageState,
    token: Rc<dyn Any>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let worker_index = scope.index();
    let total_shards = metadata.data_shards.len();
    let mut shard_counter = scope.index();

    let mut responsible_shards = vec![];
    while shard_counter < total_shards {
        responsible_shards.push(metadata.data_shards[shard_counter]);
        shard_counter += scope.peers();
    }

    println!(
        "source worker {} responsible for {:?}",
        worker_index, responsible_shards
    );

    let operator_name = format!(
        "persist_sink({})",
        responsible_shards
            .iter()
            .map(|shard| shard.to_string())
            .join(",")
    );
    let mut persist_op = OperatorBuilder::new(operator_name, scope.clone());

    let hashed_id = src_id.hashed();

    let mut input = persist_op.new_input(&source_data.inner, Exchange::new(move |_| hashed_id));

    let current_upper = Rc::clone(&storage_state.source_uppers[&src_id]);

    let weak_token = Rc::downgrade(&token);

    let persist_clients = Arc::clone(&storage_state.persist_clients);
    persist_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            capabilities.clear();
            let mut buffer = Vec::new();
            let mut stashed_batches = HashMap::new();

            let mut upper = Antichain::from_elem(Timestamp::minimum());

            let mut write_handles = vec![];
            let persist_client = persist_clients
                .lock()
                .await
                .open(metadata.persist_location)
                .await
                .expect("could not open persist client");

            for data_shard in responsible_shards {
                println!("data_shard {:?}", data_shard);
                let write = persist_client
                    .open_writer::<SourceData, (), Timestamp, Diff>(data_shard.clone())
                    .await
                    .expect("could not open persist shard");
                upper.join_assign(write.upper());
                let u = dbg!(write.upper().clone());
                write_handles.push((write, u));
            }

            // Initialize this sink's `upper` to the `upper` of the persist shard we are writing
            // to. Data from the source not beyond this time will be dropped, as it has already
            // been persisted.
            // In the future, sources will avoid passing through data not beyond this upper
            *current_upper.borrow_mut() = dbg!(upper.clone());

            while scheduler.notified().await {
                println!("input upper");
                let input_upper = dbg!(frontiers.borrow()[0].clone());

                if weak_token.upgrade().is_none() || current_upper.borrow().is_empty() {
                    println!("weak_token.upgrade().is_none() || current_upper.borrow().is_empty()");
                    return;
                }

                while let Some((_cap, data)) = input.next() {
                    println!("input.next");
                    data.swap(&mut buffer);

                    // TODO: come up with a better default batch size here
                    // (100 was chosen arbitrarily), and avoid having to make a batch
                    // per-timestamp.
                    for (row, ts, diff) in buffer.drain(..) {
                        let write_handle_idx = row.hashed() as usize % write_handles.len();
                        let (write, _upper) = &write_handles[write_handle_idx];
                        dbg!(ts);
                        dbg!(&row);
                        if dbg!(write.upper()).less_equal(&ts) {
                            println!("stashing");
                            stashed_batches
                                .entry(ts)
                                .or_insert_with(|| {
                                    let mut writes_per_handle = HashMap::new();
                                    for (i, (write, _upper)) in write_handles.iter_mut().enumerate()
                                    {
                                        writes_per_handle.insert(
                                            i,
                                            write.builder(
                                                100,
                                                Antichain::from_elem(Timestamp::minimum()),
                                            ),
                                        );
                                    }
                                    writes_per_handle
                                })
                                .get_mut(&write_handle_idx)
                                .unwrap()
                                .add(&SourceData(row), &(), &ts, &diff)
                                .await
                                .expect("invalid usage");
                        }
                    }
                }

                // See if any timestamps are done!
                // TODO(guswynn/petrosagg): remove this additional allocation
                let mut finalized_timestamps: Vec<_> = stashed_batches
                    .keys()
                    .filter(|ts| !input_upper.less_equal(ts))
                    .copied()
                    .collect();
                finalized_timestamps.sort_unstable();

                // If the frontier has advanced, we need to finalize data being written to persist
                if PartialOrder::less_than(&*current_upper.borrow(), &input_upper) {
                    println!("PartialOrder::less_than(&*current_upper.borrow(), &input_upper)");
                    // We always append, even in case we don't have any updates, because appending
                    // also advances the frontier.
                    if finalized_timestamps.is_empty() {
                        for (write, expected_upper) in write_handles.iter_mut() {
                            write
                                .append(
                                    Vec::<((SourceData, ()), Timestamp, Diff)>::new(),
                                    expected_upper.clone(),
                                    input_upper.clone(),
                                )
                                .await
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");
                            *expected_upper = input_upper.clone();
                        }

                        // advance our stashed frontier
                        *current_upper.borrow_mut() = input_upper.clone();
                        // wait for more data or a new input frontier
                        continue;
                    }

                    // `current_upper` tracks the last known upper
                    // let mut expected_upper = current_upper.borrow().clone();
                    let finalized_batch_count = finalized_timestamps.len();

                    for (i, ts) in finalized_timestamps.into_iter().enumerate() {
                        // TODO(aljoscha): Figure out how errors from this should be reported.

                        // Set the upper to the upper of the batch (which is 1 past the ts it
                        // manages) OR the new frontier if we are appending the final batch
                        let new_upper = if i == finalized_batch_count - 1 {
                            input_upper.clone()
                        } else {
                            Antichain::from_elem(ts + 1)
                        };

                        for (write_handle_idx, batch) in stashed_batches
                            .remove(&ts)
                            .expect("batch for timestamp to still be there")
                        {
                            let mut batch = batch
                                .finish(new_upper.clone())
                                .await
                                .expect("invalid usage");

                            let (write, expected_upper) = &mut write_handles[write_handle_idx];

                            write
                                .compare_and_append_batch(
                                    &mut batch,
                                    expected_upper.clone(),
                                    new_upper.clone(),
                                )
                                .await
                                .expect("cannot append updates")
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");

                            // next `expected_upper` is the one we just successfully appended
                            *expected_upper = new_upper.clone();
                        }
                    }
                    // advance our stashed frontier
                    *current_upper.borrow_mut() = input_upper.clone();
                } else {
                    // We cannot have updates without the frontier advancing
                    assert!(finalized_timestamps.is_empty());
                }
            }
        },
    )
}
