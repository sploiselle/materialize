// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;

use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::client::controller::CollectionMetadata;
use mz_storage::client::errors::DataflowError;
use mz_storage::client::sinks::{PersistSinkConnection, SinkDesc};
use mz_storage::client::sources::SourceData;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn uses_keys(&self) -> bool {
        false
    }

    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        _sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let scope = sinked_collection.scope();

        let persist_clients = Arc::clone(&compute_state.persist_clients);
        let persist_location = self.storage_metadata.persist_location.clone();

        // TODO: support case where shards != workers
        let shard_id = self.storage_metadata.shards
            [scope.index() & self.storage_metadata.shards.len()]
        .data_shard;

        let operator_name = format!("persist_sink({})", shard_id);
        let mut persist_op = OperatorBuilder::new(operator_name, scope.clone());

        let hashed_id = sink_id.hashed();

        let mut input =
            persist_op.new_input(&sinked_collection.inner, Exchange::new(move |_| hashed_id));
        let mut err_input =
            persist_op.new_input(&err_collection.inner, Exchange::new(move |_| hashed_id));

        let token = Rc::new(());
        let token_weak = Rc::downgrade(&token);

        let shared_frontier = Rc::new(RefCell::new(Antichain::from_elem(
            TimelyTimestamp::minimum(),
        )));

        compute_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&shared_frontier));

        // NOTE(aljoscha): It might be better to roll our own async operator that deals with
        // handling multiple in-flight write futures, similar to how the persist operators in
        // `operators/stream.rs` do it. That would allow us to have multiple write requests in
        // flight concurrently.
        persist_op.build_async(
            scope,
            move |_capabilities, frontiers, scheduler| async move {
                let mut buffer = Vec::new();
                let mut err_buffer = Vec::new();
                let mut stash: HashMap<_, Vec<_>> = HashMap::new();

                // TODO(aljoscha): We need to figure out what to do with error results from these calls.
                let mut write = persist_clients
                    .lock()
                    .await
                    .open(persist_location)
                    .await
                    .expect("could not open persist client")
                    .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
                    .await
                    .expect("could not open persist shard");

                while scheduler.notified().await {
                    let mut input_frontier = Antichain::new();
                    for frontier in frontiers.borrow().clone() {
                        input_frontier.extend(frontier);
                    }

                    if token_weak.upgrade().is_none() || shared_frontier.borrow().is_empty() {
                        return;
                    }

                    input.for_each(|_cap, data| {
                        data.swap(&mut buffer);

                        for ((key, value), ts, diff) in buffer.drain(..) {
                            assert!(key.is_none(), "persist_source does not support keys");
                            let row = value.expect("persist_source must have values");
                            stash.entry(ts).or_default().push((
                                (SourceData(Ok(row)), ()),
                                ts,
                                diff,
                            ));
                        }
                    });

                    err_input.for_each(|_cap, data| {
                        data.swap(&mut err_buffer);

                        for (error, ts, diff) in err_buffer.drain(..) {
                            stash.entry(ts).or_default().push((
                                (SourceData(Err(error)), ()),
                                ts,
                                diff,
                            ));
                        }
                    });

                    let mut updates = stash
                        .iter()
                        .filter(|(ts, _updates)| !input_frontier.less_equal(ts))
                        .flat_map(|(_ts, updates)| updates.iter());

                    if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                        // We always append, even in case we don't have any updates, because appending
                        // also advances the frontier.
                        let lower = shared_frontier.borrow().clone();
                        // TODO(aljoscha): Figure out how errors from this should be reported.
                        write
                            .append(updates, lower, input_frontier.clone())
                            .await
                            .expect("cannot append updates")
                            .expect("invalid/outdated upper");

                        *shared_frontier.borrow_mut() = input_frontier.clone();
                    } else {
                        // We cannot have updates without the frontier advancing.
                        assert!(updates.next().is_none());
                    }

                    stash.retain(|ts, _updates| input_frontier.less_equal(ts));
                }
            },
        );

        Some(token)
    }
}
