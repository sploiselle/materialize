We have been chatting with Kyle Kingsbury about the design of a transaction
protocol in a world where.

# Calvin-inspired

## Background

[Calvin](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) is a
distributed database transaction protocol that relies on deterministic execution
to minimize coordination between nodes.

In the prototypical Calvin implementation, global coordination occurs at most
once per epoch (e.g. every 10 ms), where each client-request-receiving service
(known as the sequencer) shares the order of execution it determined with the
services that depend on that order (known as schedulers). What gets replicated
are not the writes to the storage engine, but instead the sequence of
operations. This way, the work the schedulers complete will result in identical
states at each timestamp (i.e. after the same set of operations applied).

This parallel execution of a totally ordered set of operations not only provides
means of constructing efficient concurrent plans, but also means that any node
can get an answer from any machine that was part of the group responsible for
determining it without any coordination from _other_ machines in that group.

## Materialize design

From Calvin, Materialize could take the idea of durably replicating a set of
state changes that a consumer can "follow along" with and reach the same given
state as anyone else who followed the same set of operations.

In this proposed design, Materialize propagates all writes to a durable WAL.
(Note that unlike Calvin, we only propagate writes to the WAL and not the entire
transactional input; we are not running full stack of Materialize services
vertically.)

Storage nodes can then follow along with WAL updated and attempt to apply them
to their local state. It's important to note here that this could also mean that
storage nodes are then responsible for accepting or rejecting inputs, making
them more active participants in transaction resolution and needing to e.g.
communicate with one another.

Note: Frank says that this is already in the works to some degree, where the
storage nodes look at commands from the adapter/transactor with suspicion.

### Not detailed...

This design assumes that the writes to the WAL are serialized by some greater
force. In this design, the WAL is just a log that storage consumes and decides
what to do with. How and when a write makes it from the adapter to the WAL is
left as an exercise to the reader because the writer doesn't know.

### Example architecture

```
  client
  |    ^
 txnA  |
  |   res
  v    |                 storageA    storageB
[ADAPTER]-wA->[WAL]<-get---?
^              ^-----------get-----------?
|
|              if a?.is_ok() && b?.is_ok() {
|                  return OK to client
|              } else {
|                  A.rollback(wA.ts-1);
|                  B.rollback(wA.ts-1);
|                  return ABORT to client
|______________}
```

### Questions

- How do writes invalidate outstanding reads in e.g. `UPDATE`? I realize this
  design says that sequencing has to happen elsewhere, but just curious where we
  envision that potentially being.
- How do e.g. column constraints get handled by the txn layer? Seems like this
  moves "business logic" into storage, which I don't think will make the storage
  people happy.
- How do we rollback if we're already in the storage layer?

# CAS

## Background

This is the design Kyle has been moving forward with fleshing out, which relies
on a the WAL having a timestamp only ever written to atomically (i.e. using CAS;
if you try to update the timestamp without providing the timestamp's current
value, it fails).

## Materialize design

Materialize have one WAL that all writes go through.

When performing a read that will be followed by a write, the ADAPTER consults
the WAL and gets its current timestamp, modifying it in some way to represent a
read timestamp (vaguely representing the a value _just_ on the read side of the
write frontier). Knowing this time can be used to providean optimistic lock for
writes to the table.

When users want to write to a TVC, they'll use `append(updates, from ts, new WAL
ts)`. This implementation of `append` succeeds iff it's able to update the WAL's
ts using an atomic CAS, i.e. the WAL's current ts is `from ts`, which the writer
obtained by performing a read.

For writes that don't have a WAL ts (i.e. they never read), they can just write
at the current WAL ts++ because they could be scheduled as late as possible
(i.e. the write frontier).

If the write fails due to a failed CAS operation, you know some other transactor
wrote to the TVC after you read from it.

As writes successfully enter the WAL, storage nodes can consume them and apply
them to their disk.

Note that the benefit of the WAL here is that it permits multi-table write
transactions.
