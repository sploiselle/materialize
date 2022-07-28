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

use mz_ore::cast::CastFrom;
use mz_persist::location::ExternalError;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{ListenEvent, ReaderEnrichedHollowBatch};
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::async_op;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::controller::CollectionMetadata;
use crate::source::YIELD_INTERVAL;
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

/// Creates a new source that reads from a persist shard.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G>(
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

    let persist_clients_stream = Arc::<Mutex<PersistClientCache>>::clone(&persist_clients);
    let persist_location_stream = metadata.persist_location.clone();
    let data_shard = metadata.data_shard.clone();
    let as_of_stream = as_of;

    // This source is split as such:
    // 1. Sets up `async_stream`, which only yields data on one worker.
    // 2. A timely source operator which continuously reads from that stream,
    //    and distributes the data (hollow batches) among workers.
    // 3. A timely operator which downloads the batch's contents from S3, and
    //    outputs them to a stream.

    // This is a generator that sets up an async `Stream` that can be continously polled to get the
    // values that are `yield`-ed from it's body.
    let async_stream = async_stream::try_stream!({
        // Worker 0 is responsible for distributing splits from listen
        if worker_index != 0 {
            trace!("We are not worker 0, exiting...");
            return;
        }

        let read = persist_clients_stream
            .lock()
            .await
            .open(persist_location_stream)
            .await
            .expect("could not open persist client")
            .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(data_shard.clone())
            .await
            .expect("could not open persist shard");

        let mut subscription = read
            .subscribe(as_of_stream)
            .await
            .expect("cannot serve requested as_of");

        loop {
            yield subscription.next().await;
        }
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (inner, token) =
        crate::source::util::source(scope, "persist_source".to_string(), move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(waker_activator);

            let mut current_ts = 0;

            move |cap_set, output| {
                let mut context = Context::from_waker(&waker);

                let mut i = 0;

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(batch)) => {
                            let session_cap = cap_set.delayed(&current_ts);
                            let mut session = output.session(&session_cap);
                            let progress = batch.generate_progress();

                            session.give((i, batch));

                            // Round robin
                            i = (i + 1) % peers;
                            if let Some(frontier) = progress {
                                cap_set.downgrade(frontier.iter());
                                match frontier.into_option() {
                                    Some(ts) => {
                                        current_ts = ts;
                                    }
                                    None => {
                                        cap_set.downgrade(&[]);
                                        return;
                                    }
                                }
                            }
                        }
                        Some(Err::<_, ExternalError>(e)) => {
                            panic!("unexpected error from persist {e}")
                        }
                        // We never expect any further output from
                        // `pinned_stream`, so propagate that information
                        // downstream.
                        None => {
                            cap_set.downgrade(&[]);
                            return;
                        }
                    }
                }
            }
        });

    let mut builder = OperatorBuilder::new(
        format!(
            "persist_source: sharded reader {} of {:?}",
            worker_index, source_id
        ),
        scope.clone(),
    );
    let operator_info = builder.operator_info();
    let dist = |&(i, _): &(usize, ReaderEnrichedHollowBatch<Timestamp>)| Timestamp::cast_from(i);
    let mut input = builder.new_input(&inner, Exchange::new(dist));
    let (mut output, output_stream) = builder.new_output();

    // Bound execution of operator to prevent a single operator from hogging
    // the CPU if there are many messages to process
    let activator = scope.activator_for(&operator_info.address[..]);

    builder.build_async(
        scope.clone(),
        async_op!(|initial_capabilities, _frontiers| {
            let read = persist_clients
                .lock()
                .await
                .open(metadata.persist_location.clone())
                .await
                .expect("could not open persist client")
                .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                    data_shard.clone(),
                )
                .await
                .expect("could not open persist shard");

            initial_capabilities.clear();

            let mut output_handle = output.activate();

            let timer = Instant::now();

            while let Some((cap, data)) = input.next() {
                let cap = cap.retain();
                for (_idx, batch) in data.iter() {
                    for update in read
                        .fetch_batch(batch.clone())
                        .await
                        .expect("fetching batches cannot fail")
                    {
                        match update {
                            ListenEvent::Updates(mut updates) => {
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

    let (ok_stream, err_stream) = output_stream.ok_err(|x| match x {
        ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => Ok((row, ts, diff)),
        ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => Err((err, ts, diff)),
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
