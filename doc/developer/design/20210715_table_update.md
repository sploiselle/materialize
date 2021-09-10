# `UPDATE` and `DELETE` for tables

## Summary

Support `UPDATE` and `DELETE` for tables. These cannot be used in transactions
and are serialized along with `INSERT`s.

## Goals

Add a framework for supporting transactional table operations that perform
writes based on the result of a read. This framework is designed to be
serializable, and so is safe when used by multiple concurrent SQL sessions.

## Non-Goals

- General transactions with interleaved reads and writes
- High performance under heavy concurrent load

## Description

`UPDATE`, `DELETE`, `UPSERT`, `INSERT ON CONFLICT`, and unique key constraints
are similar operations: a read followed by a write depending on the results of
the read.
- `DELETE` removes all read rows
- `UPDATE` atomically removes all read rows and adds a new row for each removed
- Unique key constraints will (during `UPDATE` and `INSERT`) do a read looking
  for any rows that might already exist, and do nothing (or error) if there are
  any result rows
- `UPSERT` and `INSERT ON CONFLICT` will do the same as unique key constraints
  above, but will instead not error and either do nothing for each conflict row
  or remove it and add a new one

We can add basic support for `UPDATE` and `DELETE` with a new plan node called
`ReadThenWrite` that can be configured to do a read and then arbitrary behavior
on its results. We can add support for the others at a later date.

`ReadThenWrite` is a node that performs a `Peek` at some time `t1`. Once the Peek
has results ready, they are read and a set of diffs is produced which are sent
at some time `t2` where `t1 < t2`.

To support our goal of strict serializability, we need to ensure that no writes
to tables affected by `ReadThenWrite` happen between `t1` and `t2`. Do this by
teaching Insert and `ReadThenWrite` to serialize themselves by checking if
another write has started and adding themselves to a queue if so, otherwise
marking themselves as the in-progress write and continuing. At transaction end,
if the transaction's session is the in-progress write and there's a queued
write, move the queued write to the coordinator's command queue, scheduling it
for execution. It is safe to use `t2` as the write time since the serialization
prevents writes to the read table between `t1` and `t2`, so the read is still
correct as of `t2`.

We can optimize `INSERT`'s performance by only checking for other in-progress
writes until its commit time. This allows for `INSERT`-only workloads to
serialize by nature of the single-threaded coordinator, just like the system's
state prior to merging this code.

We want to prevent the case where one user opens a write transaction and forces
all other write transactions for the first user to issue a `COMMIT`. The design
above achieves this because `INSERT` only needs to serialize itself after the
user has issued a `COMMIT` statement, and `ReadThenWrite` has no user
interaction between its read and write. Additionally, `ReadThenWrite` must be
excluded from explicit/multi-statement transactions.

### Implementing global write lock

In the initial design of this code, we relied on keeping track of the connection
ID of the session that held the write lock as an `Option<u32>`. If the write
lock was...
- `Some`, only the connection with that ID was allowed to enter the critical
  sections; others had to enqueue themselves and wait to be dequeued by the
  lock's owner
- `None`, entering the critical section saved the connection's ID as the
  connection holding the lock.

When the connection holding the lock was leaving the critical section, it was
responsible for dequeuing an operation that was awaiting the write lock.

A successive commit to this code, instead refactors the global write lock to use
a `Mutex` instead of an `Option<u32>` because a `Mutex` offers exactly the
semantics we want to serialize writes:
- It limits a critical path to one entrant at a time
- Signals those waiting on the lock of its availability and lets one of them
    proceed

While the mutex I chose comes from `tokio::sync`, mutexes are well-known,
well-tested, idiomatic tools from standard libraries of many programming
languages, so using one has the advantage of giving us exactly what we want with
very little work, and is generally less fallible than trying to hand-roll the
same semantics using another implemenation.

By using a mutex, if the session holding the write lock gets dropped, the lock
is released. To reach parity with the `Option<u32>` approach, we would need to
implement an "unlock" operation on `Drop`, at which point we're basically
handrolling a mutex.

#### Considerations

**Implementation**

I chose `tokio::sync::Mutex` because its
[documentation](https://docs.rs/tokio/1.11.0/tokio/sync/struct.Mutex.html) states:

> [`tokio::sync::Mutex`'s] lock guard is designed to be held across `.await`
> points

And we want to hold this lock across at least one `await` point during commits.
There are more details in the documentation.

While this has a potential performance implication, performance is expressly not
a goal of this code.

**Where to put the mutex handle**

Because e.g. `ReadThenWrite` nodes are "long-lived" calls, the simplest place to
share data (i.e. the mutex's handle) for the life of the function call is on the
`Session`. However, because `Session` is even-longer-lived, this means that
there is not a naturally occurring place for the mutex to release itself, so you
must manually release the lock.

While this isn't ideal, it isn't inferior to the prior iteration's design, which
still required manual unlocking, but without the benefit of unlocking on drop.

**Deferred work**

This iteration of the code works similarly to the prior version, where deferred
work is placed on a `VecDeque`. When an operation fails to obtain the lock, it's
parked in a greenthread and uses `internal_cmd_tx` to signal the lock's
availability.

The previous iteration of this code removed the deferred work from the internal
`VecDeque` and sent the entire plan through `internal_cmd_tx`. However, the
mutex-backed implementation simply lets us send along the mutex's handle and
lets the coordinator itself handle dequeuing the work and installing the handle
on the appropriate session.

There is also an alternate design that would be beneficial if we have multiple
write locks (e.g. one per table), where installing the handles occur in the
greenthread, i.e. wait on the mutex in the greenthread, install the handle on
the session once it become available, and then message the entire plan to
execute. However, this design is a little messier when dealing with
cancellations, so I don't think is worth pursuing at this point.

I believe the implemented approach also provides a linearizable guarantee: items
are only ever enqueued in the order they're received, and dequeued unless
canceled. The approach mentioned directly above this would not provide
linearizability because acquiring the lock depends on the greenthread's
scheduling and not the order in which the coordinator received the request.

## Alternatives

Instead of maintaining a global write lock for all tables, it could be per table
or perhaps per row. This is an improvement that could be made if we need more
throughput under concurrent workloads.

## Open questions

Is optimistic locking possible and better? This is another alternative that we
can explore if needed.
