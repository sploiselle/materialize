// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_util::Stream as FuturesStream;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::Mutex;
use tracing::trace;

use mz_persist::location::ExternalError;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{ListenEvent, ReaderEnrichedHollowBatch};
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::async_op;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::controller::CollectionMetadata;
use crate::source::{SourceStatus, YIELD_INTERVAL};
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

/// Creates a new source that reads from a persist shard.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
//
// TODO(aljoscha): We need to change the `shard_id` parameter to be a `Vec<ShardId>` and teach the
// operator to concurrently poll from multiple `Listen` instances. This will require us to put in
// place the code that allows selecting from multiple `Listen`s, potentially by implementing async
// `Stream` for it and then using `select_all`. And it will require us to properly combine the
// upper frontier from all shards.
// pub fn persist_source<G>(
//     scope: &G,
//     persist_clients: Arc<Mutex<PersistClientCache>>,
//     metadata: CollectionMetadata,
//     as_of: Antichain<Timestamp>,
// ) -> (
//     Stream<G, (Row, Timestamp, Diff)>,
//     Stream<G, (DataflowError, Timestamp, Diff)>,
//     Rc<dyn Any>,
// )
// where
//     G: Scope<Timestamp = mz_repr::Timestamp>,
// {
//     let worker_index = scope.index();

//     // This source is split into two parts: a first part that sets up `async_stream` and a timely
//     // source operator that the continuously reads from that stream.
//     //
//     // It is split that way because there is currently no easy way of setting up an async source
//     // operator in materialize/timely.

//     // This is a generator that sets up an async `Stream` that can be continously polled to get the
//     // values that are `yield`-ed from it's body.
//     let async_stream = async_stream::try_stream!({
//         // We are reading only from worker 0. We can split the work of reading from the snapshot to
//         // multiple workers, but someone has to distribute the splits. Also, in the glorious
//         // STORAGE future, we will use multiple persist shards to back a STORAGE collection. Then,
//         // we can read in parallel, by distributing shard reading amongst workers.
//         if worker_index != 0 {
//             trace!("We are not worker 0, exiting...");
//             return;
//         }

//         let read = persist_clients
//             .lock()
//             .await
//             .open(metadata.persist_location)
//             .await
//             .expect("could not open persist client")
//             .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(metadata.data_shard)
//             .await
//             .expect("could not open persist shard");

//         let mut snapshot_iter = read
//             .snapshot(as_of.clone())
//             .await
//             .expect("cannot serve requested as_of");

//         // First, yield all the updates from the snapshot.
//         while let Some(next) = snapshot_iter.next().await {
//             yield ListenEvent::Updates(next);
//         }

//         // Then, listen continuously and yield any new updates. This loop is expected to never
//         // finish.
//         let mut listen = read
//             .listen(as_of)
//             .await
//             .expect("cannot serve requested as_of");

//         loop {
//             for event in listen.next().await {
//                 yield event;
//             }
//         }
//     });

//     let mut pinned_stream = Box::pin(async_stream);

//     let (timely_stream, token) =
//         crate::source::util::source(scope, "persist_source".to_string(), move |info| {
//             let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
//             let waker = futures_util::task::waker(waker_activator);
//             let activator = scope.activator_for(&info.address[..]);

//             // There is a bit of a mismatch: listening on a ReadHandle will give us an Antichain<T>
//             // as a frontier in `Progress` messages while a timely source usually only has a single
//             // `Capability` that it can downgrade. We can work around that by using a
//             // `CapabilitySet`, which allows downgrading the contained capabilities to a frontier
//             // but `util::source` gives us both a vanilla `Capability` and a `CapabilitySet` that
//             // we have to keep downgrading because we cannot simply drop the one that we don't
//             // need.
//             let mut current_ts = 0;

//             move |cap, output| {
//                 let mut context = Context::from_waker(&waker);
//                 // Bound execution of operator to prevent a single operator from hogging
//                 // the CPU if there are many messages to process
//                 let timer = Instant::now();

//                 while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
//                     match item {
//                         Some(Ok(ListenEvent::Progress(upper))) => match upper.into_option() {
//                             Some(ts) => {
//                                 current_ts = ts;
//                                 cap.downgrade(&ts);
//                             }
//                             None => return SourceStatus::Done,
//                         },
//                         Some(Ok(ListenEvent::Updates(mut updates))) => {
//                             // This operator guarantees that its output has been advanced by `as_of.
//                             // The persist SnapshotIter already has this contract, so nothing to do
//                             // here.
//                             let cap = cap.delayed(&current_ts);
//                             let mut session = output.session(&cap);
//                             session.give_vec(&mut updates);
//                         }
//                         Some(Err::<_, ExternalError>(e)) => {
//                             // TODO(petrosagg): error handling
//                             panic!("unexpected error from persist {e}");
//                         }
//                         None => return SourceStatus::Done,
//                     }
//                     if timer.elapsed() > YIELD_INTERVAL {
//                         activator.activate();
//                         break;
//                     }
//                 }

//                 SourceStatus::Alive
//             }
//         });

//     let (ok_stream, err_stream) = timely_stream.ok_err(|x| match x {
//         ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => Ok((row, ts, diff)),
//         ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => Err((err, ts, diff)),
//         // TODO(petrosagg): error handling
//         _ => panic!("decoding failed"),
//     });

//     let token = Rc::new(token);

//     (ok_stream, err_stream, token)
// }

pub fn persist_source_listen_split<G>(
    scope: &G,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    metadata: CollectionMetadata,
    as_of: Antichain<Timestamp>,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let worker_index = scope.index();
    let peers = scope.peers();

    let data_shard = metadata.data_shard.clone();

    let persist_clients_moveable = persist_clients.clone();
    let persist_location_moveable = metadata.persist_location.clone();
    let as_of_moveable = as_of.clone();

    // This source is split into two parts: a first part that sets up `async_stream` and a timely
    // source operator that the continuously reads from that stream.
    //
    // It is split that way because there is currently no easy way of setting up an async source
    // operator in materialize/timely.

    // This is a generator that sets up an async `Stream` that can be continously polled to get the
    // values that are `yield`-ed from it's body.
    let async_stream = async_stream::try_stream!({
        // Worker 0 is responsible for distributing splits from listen
        if worker_index != 0 {
            trace!("We are not worker 0, exiting...");
            return;
        }

        let persist_client = persist_clients_moveable
            .lock()
            .await
            .open(persist_location_moveable)
            .await
            .expect("could not open persist client")
            .clone();

        let mut read = persist_client
            .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(data_shard.clone())
            .await
            .expect("could not open persist shard");

        // Aggressively downgrade `since`, to not hold back compaction.
        read.downgrade_since(as_of_moveable.clone()).await;

        let mut subscription = read
            .subscribe(as_of_moveable)
            .await
            .expect("cannot serve requested as_of");

        loop {
            for batch in subscription
                .next()
                .await
                .expect("cannot serve requested as_of")
            {
                yield batch;
            }
        }
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (inner, token) =
        crate::source::util::source(scope, "persist_source".to_string(), move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(waker_activator);

            // There is a bit of a mismatch: listening on a ReadHandle will give us an Antichain<T>
            // as a frontier in `Progress` messages while a timely source usually only has a single
            // `Capability` that it can downgrade. We can work around that by using a
            // `CapabilitySet`, which allows downgrading the contained capabilities to a frontier
            // but `util::source` gives us both a vanilla `Capability` and a `CapabilitySet` that
            // we have to keep downgrading because we cannot simply drop the one that we don't
            // need.

            let mut current_ts = 0;

            move |cap, output| {
                let mut context = Context::from_waker(&waker);

                let mut i = 0;

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(batch)) => {
                            let session_cap = cap.delayed(&current_ts);
                            let mut session = output.session(&session_cap);
                            let progress = batch.generate_progress();

                            session.give((i, batch));

                            // Round robin
                            i = (i + 1) % peers;
                            println!("op_0: capability is {:?}", cap);
                            if let Some(frontier) = dbg!(progress) {
                                match frontier.into_option() {
                                    Some(ts) => {
                                        cap.downgrade(&ts);
                                        println!("op_0: capability downgraded to {:?}", cap);
                                        current_ts = ts;
                                    }
                                    None => return SourceStatus::Done,
                                }
                            }
                        }
                        Some(Err::<_, ExternalError>(e)) => {
                            panic!("unexpected error from persist {e}")
                        }
                        None => return SourceStatus::Done,
                    }
                }

                SourceStatus::Alive
            }
        });

    let mut builder = OperatorBuilder::new("partitioned reader".to_string(), scope.clone());
    let operator_info = builder.operator_info();
    let dist = |&(i, _): &(usize, ReaderEnrichedHollowBatch<u64>)| i as u64;
    let mut input = builder.new_input(&inner, Exchange::new(dist));
    let (mut output, output_stream) = builder.new_output();

    let mut current_ts = 0;

    // // Bound execution of operator to prevent a single operator from hogging
    // // the CPU if there are many messages to process
    let activator = scope.activator_for(&operator_info.address[..]);

    builder.build_async(
        scope.clone(),
        async_op!(|initial_capabilities, _frontiers| {
            let persist_client = persist_clients
                .lock()
                .await
                .open(metadata.persist_location.clone())
                .await
                .expect("could not open persist client")
                .clone();

            let read = persist_client
                .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                    data_shard.clone(),
                )
                .await
                .expect("could not open persist shard");

            initial_capabilities.clear();

            let mut output_handle = output.activate();

            let timer = Instant::now();

            while let Some((cap, data)) = input.next() {
                println!("op_1 cap {:?}", cap);
                // let mut cap = cap.retain();
                for (_idx, batch) in data.iter() {
                    for update in read
                        .fetch_batch(batch.clone())
                        .await
                        .expect("listen iter cannot fail")
                    {
                        match dbg!(update) {
                            ListenEvent::Updates(mut updates) => {
                                // This operator guarantees that its output has been advanced by `as_of.
                                // The persist SnapshotIter already has this contract, so nothing to do
                                // here.
                                let cap = cap.delayed(&current_ts);
                                let mut session = output_handle.session(&cap);
                                session.give_vec(&mut updates);
                            }
                            ListenEvent::Progress(..) => unreachable!(
                                "ReadHandle::fetch_batch does not emit ListenEvent::Progress"
                            ),
                        }

                        if timer.elapsed() > YIELD_INTERVAL {
                            activator.activate();
                            break;
                        }
                    }
                }
            }
            false
        }),
    );

    let (ok_stream, err_stream) = output_stream.ok_err(|x| match dbg!(x) {
        ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => Ok((row, ts, diff)),
        ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => Err((err, ts, diff)),
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
