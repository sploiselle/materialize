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
use mz_persist_client::cache::PersistClientCache;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::Mutex;
use tokio_stream::{StreamExt, StreamMap};
use tracing::info;

use mz_persist::location::ExternalError;
use mz_persist_client::read::ListenEvent;
use mz_repr::{Diff, Row, Timestamp};

use crate::client::controller::CollectionMetadata;
use crate::client::errors::DataflowError;
use crate::client::sources::SourceData;
use crate::source::{SourceStatus, YIELD_INTERVAL};

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
    let total_shards = metadata.shards.len();
    let mut shard_counter = scope.index();

    let mut responsible_shards = vec![];
    while shard_counter < total_shards {
        responsible_shards.push(metadata.shards[shard_counter].data_shard);
        shard_counter += scope.peers();
    }

    println!(
        "\n\navailable shards {}: {:?}",
        metadata.shards.len(),
        metadata.shards
    );

    println!(
        "\n\nresponsible_shards {}: {:?}",
        responsible_shards.len(),
        responsible_shards
    );

    let mut progress_by_shard_idx = std::collections::BTreeMap::new();

    for i in 0..responsible_shards.len() {
        progress_by_shard_idx.insert(i, as_of.clone());
    }

    // This source is split into two parts: a first part that sets up `async_stream` and a timely
    // source operator that the continuously reads from that stream.
    //
    // It is split that way because there is currently no easy way of setting up an async source
    // operator in materialize/timely.

    // This is a generator that sets up an async `Stream` that can be continously polled to get the
    // values that are `yield`-ed from it's body.
    let async_stream = async_stream::try_stream!({
        let mut listens = Vec::with_capacity(responsible_shards.len());
        for (idx, shard_id) in responsible_shards.into_iter().enumerate() {
            println!("READING FROM {:?}", shard_id);
            let mut read = persist_clients
                .lock()
                .await
                .open(metadata.persist_location.clone())
                .await
                .expect("could not open persist client")
                .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            /// Aggressively downgrade `since`, to not hold back compaction.
            read.downgrade_since(as_of.clone()).await;

            let mut snapshot_iter = read
                .snapshot(as_of.clone())
                .await
                .expect("starting cannot serve requested as_of");

            // First, yield all the updates from the snapshot.
            while let Some(next) = snapshot_iter.next().await {
                yield (idx, ListenEvent::Updates(next));
            }

            // Then, listen continously and yield any new updates. This loop is expected to never
            // finish.
            listens.push((
                idx,
                Box::pin(
                    read.listen(as_of.clone())
                        .await
                        .expect("cannot serve requested as_of")
                        .into_stream(),
                ),
            ))
        }

        let mut listen_agg = listens.into_iter().collect::<StreamMap<_, _>>();

        loop {
            for (idx, event) in listen_agg.next().await {
                // TODO(petrosagg): We are incorrectly NOT downgrading the since frontier of this
                // read handle which will hold back compaction in persist. This is currently a
                // necessary evil to avoid too much contension on persist's consensus
                // implementation.
                //
                // Once persist supports compaction and/or has better performance the code below
                // should be enabled.
                // if let ListenEvent::Progress(upper) = &event {
                //     read.downgrade_since(upper.clone()).await;
                // }
                yield (idx, event);
            }
        }
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (timely_stream, token) =
        crate::source::util::source(scope, "persist_source".to_string(), move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(waker_activator);
            let activator = scope.activator_for(&info.address[..]);

            move |cap_set, output| {
                let mut context = Context::from_waker(&waker);
                // Bound execution of operator to prevent a single operator from hogging
                // the CPU if there are many messages to process
                let timer = Instant::now();

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok((shard_idx, ListenEvent::Progress(upper)))) => {
                            if upper.is_empty() {
                                progress_by_shard_idx.remove(&shard_idx);
                                if progress_by_shard_idx.is_empty() {
                                    return SourceStatus::Done;
                                }
                            } else {
                                progress_by_shard_idx.insert(shard_idx, upper.clone());
                                let lower_bound: Antichain<u64> = Antichain::from_iter(
                                    progress_by_shard_idx.values().cloned().flatten(),
                                );
                                cap_set.downgrade(lower_bound.iter().min().unwrap());
                            }
                        }
                        Some(Ok((shard_idx, ListenEvent::Updates(mut updates)))) => {
                            // This operator guarantees that its output has been advanced by `as_of.
                            // The persist SnapshotIter already has this contract, so nothing to do
                            // here.

                            println!("shard_idx {shard_idx}: updates {:?}", updates);

                            if updates.is_empty() {
                                continue;
                            }

                            // Determine the minimum timestamp and emit all
                            // updates at that timestamp. We could be more
                            // fine-grained and first partition the updates by
                            // timestamp. But then we'd have multiple give_vec()
                            // calls below.
                            // TODO(aljoscha): I don't like having to iterate
                            // through all updates in order to determine a
                            // timestamp. Maybe persist `listen()` should emit a
                            // timestamp for the batch, similar to how inputs
                            // work in
                            // timely.
                            let ts = updates
                                .iter()
                                .map(|(_update, ts, _diff)| ts)
                                .min()
                                .expect("no updates");

                            let cap = cap_set.delayed(ts);
                            let mut session = output.session(&cap);
                            session.give_vec(&mut updates);
                        }
                        Some(Err::<_, ExternalError>(e)) => {
                            // TODO(petrosagg): error handling
                            panic!("unexpected error from persist {e}");
                        }
                        None => return SourceStatus::Done,
                    }
                    if timer.elapsed() > YIELD_INTERVAL {
                        activator.activate();
                        break;
                    }
                }

                SourceStatus::Alive
            }
        });

    let (ok_stream, err_stream) = timely_stream.ok_err(move |x| match x {
        ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => {
            info!("persist_source {worker_index}: Ok({row})");
            Ok((row, ts, diff))
        }
        ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => {
            info!("persist_source {worker_index}: Err({err})");
            Err((err, ts, diff))
        }
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
