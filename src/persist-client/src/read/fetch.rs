// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fetching batches of data from persist's backing store

use std::fmt::Debug;
use std::time::SystemTime;

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug_span, info, instrument, trace_span, Instrument};

use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::Blob;
use mz_persist::retry::Retry;
use mz_persist_types::{Codec, Codec64};

use crate::error::InvalidUsage;
use crate::r#impl::machine::retry_external;
use crate::r#impl::metrics::Metrics;
use crate::r#impl::paths::PartialBlobKey;
use crate::read::HollowBatchReaderMetadata;
use crate::ShardId;

use super::{ConsumedBatch, ReadHandle, ReaderEnrichedHollowBatch};

/// Capable of fetch batches, and appropriately handling the metadata on
/// [`ReaderEnrichedHollowBatch`]es.
#[derive(Debug)]
pub struct BatchFetcher<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) handle: ReadHandle<K, V, T, D>,
}

impl<K, V, T, D> BatchFetcher<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(super) fn new(handle: ReadHandle<K, V, T, D>) -> Self {
        BatchFetcher { handle }
    }

    /// Trade in an exchange-able [ReaderEnrichedHollowBatch] for the data it
    /// represents.
    ///
    /// The [`ConsumedBatch`] returned after fetching must be passed back to
    /// issuer ([`ReadHandle::process_consumed_batch`],
    /// [`Subscribe::process_consumed_batch`]), so that it can properly handle
    /// internal bookkeeping.
    #[instrument(level = "debug", skip_all, fields(shard = %self.handle.machine.shard_id()))]
    pub async fn fetch_batch(
        &mut self,
        batch: ReaderEnrichedHollowBatch<T>,
    ) -> Result<
        (
            ConsumedBatch,
            Vec<((Result<K, String>, Result<V, String>), T, D)>,
        ),
        InvalidUsage<T>,
    > {
        if batch.shard_id != self.handle.machine.shard_id() {
            return Err(InvalidUsage::BatchNotFromThisShard {
                batch_shard: batch.shard_id,
                handle_shard: self.handle.machine.shard_id(),
            });
        }

        let batch_seqno = batch.seqno();
        let mut updates = Vec::new();
        for key in batch.batch.keys.iter() {
            self.handle.maybe_heartbeat_reader().await;
            fetch_batch_part(
                &batch.shard_id,
                self.handle.blob.as_ref(),
                &self.handle.metrics,
                &key,
                &batch.batch.desc.clone(),
                |k, v, mut t, d| {
                    match &batch.reader_metadata {
                        HollowBatchReaderMetadata::Listen {
                            as_of,
                            until,
                            since: _,
                        } => {
                            // This time is covered by a snapshot
                            if !as_of.less_than(&t) {
                                return;
                            }

                            // Because of compaction, the next batch we get might also
                            // contain updates we've already emitted. For example, we
                            // emitted `[1, 2)` and then compaction combined that batch
                            // with a `[2, 3)` batch into a new `[1, 3)` batch. If this
                            // happens, we just need to filter out anything < the
                            // frontier. This frontier was the upper of the last batch
                            // (and thus exclusive) so for the == case, we still emit.
                            if !until.less_equal(&t) {
                                return;
                            }
                        }
                        HollowBatchReaderMetadata::Snapshot { as_of } => {
                            // This time is covered by a listen
                            if as_of.less_than(&t) {
                                return;
                            }
                            t.advance_by(as_of.borrow())
                        }
                    }

                    let k = self.handle.metrics.codecs.key.decode(|| K::decode(k));
                    let v = self.handle.metrics.codecs.val.decode(|| V::decode(v));
                    let d = D::decode(d);
                    updates.push(((k, v), t, d));
                },
            )
            .await;
        }

        if let HollowBatchReaderMetadata::Listen { since, .. } = batch.reader_metadata {
            self.handle.maybe_downgrade_since(&since).await;
        }

        Ok((ConsumedBatch(batch_seqno), updates))
    }
}

pub(crate) async fn fetch_batch_part<T, UpdateFn>(
    shard_id: &ShardId,
    blob: &(dyn Blob + Send + Sync),
    metrics: &Metrics,
    key: &PartialBlobKey,
    registered_desc: &Description<T>,
    mut update_fn: UpdateFn,
) where
    T: Timestamp + Lattice + Codec64,
    UpdateFn: FnMut(&[u8], &[u8], T, [u8; 8]),
{
    let mut retry = metrics
        .retries
        .fetch_batch_part
        .stream(Retry::persist_defaults(SystemTime::now()).into_retry_stream());
    let get_span = debug_span!("fetch_batch::get");
    let value = loop {
        let value = retry_external(&metrics.retries.external.fetch_batch_get, || async {
            blob.get(&key.complete(shard_id)).await
        })
        .instrument(get_span.clone())
        .await;
        match value {
            Some(x) => break x,
            // If the underlying impl of blob isn't linearizable, then we
            // might get a key reference that that blob isn't returning yet.
            // Keep trying, it'll show up.
            None => {
                // This is quite unexpected given that our initial blobs _are_
                // linearizable, so always log at info.
                info!(
                    "unexpected missing blob, trying again in {:?}: {}",
                    retry.next_sleep(),
                    key
                );
                retry = retry.sleep().await;
            }
        };
    };
    drop(get_span);

    trace_span!("fetch_batch::decode").in_scope(|| {
        let batch = metrics
            .codecs
            .batch
            .decode(|| BlobTraceBatchPart::decode(&value))
            .map_err(|err| anyhow!("couldn't decode batch at key {}: {}", key, err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");

        // Drop the encoded representation as soon as we can to reclaim memory.
        drop(value);

        // There are two types of batches in persist:
        // - Batches written by a persist user (either directly or indirectly
        //   via BatchBuilder). These always have a since of the minimum
        //   timestamp and may be registered in persist state with a tighter set
        //   of bounds than are inline in the batch (truncation). To read one of
        //   these batches, all data physically in the batch but outside of the
        //   truncated bounds must be ignored. Not every user batch is
        //   truncated.
        // - Batches written by compaction. These always have an inline desc
        //   that exactly matches the one they are registered with. The since
        //   can be anything.
        let inline_desc = decode_inline_desc(&batch.desc);
        let needs_truncation = inline_desc.lower() != registered_desc.lower()
            || inline_desc.upper() != registered_desc.upper();
        if needs_truncation {
            assert!(
                PartialOrder::less_equal(inline_desc.lower(), registered_desc.lower()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            assert!(
                PartialOrder::less_equal(registered_desc.upper(), inline_desc.upper()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
            // As mentioned above, batches that needs truncation will always have a
            // since of the minimum timestamp. Technically we could truncate any
            // batch where the since is less_than the output_desc's lower, but we're
            // strict here so we don't get any surprises.
            assert_eq!(
                inline_desc.since(),
                &Antichain::from_elem(T::minimum()),
                "key={} inline={:?} registered={:?}",
                key,
                inline_desc,
                registered_desc
            );
        } else {
            assert_eq!(
                &inline_desc, registered_desc,
                "key={} inline={:?} registered={:?}",
                key, inline_desc, registered_desc
            );
        }

        for chunk in batch.updates {
            for ((k, v), t, d) in chunk.iter() {
                let t = T::decode(t);

                // This filtering is really subtle, see the comment above for
                // what's going on here.
                if needs_truncation {
                    if !registered_desc.lower().less_equal(&t) {
                        continue;
                    }
                    if registered_desc.upper().less_equal(&t) {
                        continue;
                    }
                }

                update_fn(k, v, t, d);
            }
        }
    })
}

// TODO: This goes away the desc on BlobTraceBatchPart becomes a Description<T>,
// which should be a straightforward refactor but it touches a decent bit.
fn decode_inline_desc<T: Timestamp + Codec64>(desc: &Description<u64>) -> Description<T> {
    fn decode_antichain<T: Timestamp + Codec64>(x: &Antichain<u64>) -> Antichain<T> {
        Antichain::from(
            x.elements()
                .iter()
                .map(|x| T::decode(x.to_le_bytes()))
                .collect::<Vec<_>>(),
        )
    }
    Description::new(
        decode_antichain(desc.lower()),
        decode_antichain(desc.upper()),
        decode_antichain(desc.since()),
    )
}
