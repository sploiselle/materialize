Table txn design extension

Supporting `SELECT` and `INSERT` in the same transaction over user-defined
tables is...
- A generally desirable feature to provide more complex expressions within a
  transaction, e.g. reading your own writes during a transaction
- Required to some degree to support Fivetran, which requires supporting at
    least `DELETE`s followed by `INSERT`s Note to readers that we model DELETE
    operations as a "Read then Write," which placing it in a transaction
    requires some extension of the scope of our current transaction semantics.

## Invariants we must maintain

Materialize aims to be "linearizable," which you can find a great deal of detail
about [here](https://github.com/MaterializeInc/materialize/pull/7355/files).

## An overview of the current design

Without getting too much into the "why," it's possible to enumerate the
properties of Materialize's current transactional semantics and ensure that any
changes to it retain the same invariants--this should, in theory, transitively
provide the same transactional guarantees.

### Ensure writes are only visible to others on commit

Differential has no notion of a provisional write, i.e. once a write it sent to
dataflow, it's visible to all users.

To ensure our writes' rows don't appear to other users before our commit, we
buffer them on the `Session`, and only send them to dataflow on commit.

### Prevent writes based on reads from having their read values change

In general, we prevent writes based on reads from occurring by disallowing
writes and reads in the same transaction. Transactions in Materialize are
currently write-only or read-only.

The exception to this rule is the `ReadThenWrite` plan, which performs a read
followed by a write, but holds a global write lock for the length of its
execution (i.e. before the write occurs), ensuring no values it reads can
change. `ReadThenWrite` operations are also prohibited in general transactions
(i.e. `BEGIN...COMMIT`), which makes the first paragraph still strictly true.
Note that when committing writes in transactions, this same write lock gets
held, albeit it's only given to the `Session` later in the process.

### Serialized writes

Given the coordinator's single-threadedness and our lack of support for
side-effecting transactions, our transaction histories are trivially serialized.

## Challenges with the current design
The current design expressly forbids the presence of both reads and writes in
the same transaction, which prevents e.g. using `DELETE` inside a transaction, or
more generally just any kind of `INSERT`/`SELECT` mixture. So the current design
must be wholly reconsidered into something new that permits blended operations,
while maintaining linearizability.

One expectation of expanding transactions to support blended operations is
letting transactions read values they themselves have written, commonly referred
to as "read your own writes." The current design stuffing rows in `Session`
prevents writes from being seen, but also prevents them from being meaningfully
read by the current `Session` either.

# Implementation proposal

We can support blended operation transactions while maintaining the same
invariants we currently do.

### Ensure writes are only visible to others on commit

Rather than buffering written rows on `Session`, we can buffer writes in
temporary tables, which will be inaccessible to other users. This lets
`Session`s read values its written from within the transaction without leaking
those to others.

### Prevent writes based on reads from having their read values change

When beginning transactions, we can can clone all items' current timestamps. If
a write occurs to an item we've read from (i.e. its timestamp has advanced)
between our transaction start and commit times, we can either:

- Surface a retryable error to the client.
- Hold a write lock across all read-from objects and re-execute the transaction
  on the client's behalf.

The only reasonable alternative here would be to hold a global write lock
whenever a transaction performs its first read. This seems like a hostile user
experience, so not something I have pored much thought into.

### Serialized writes

The coordinator remains single-threaded, which means serializing writes remains
straightforward, given our ability to abort transactions on read-write conflicts.

## Details

### Ensure writes are only visible to others on commit

Rather than buffering our provisional writes in an unreadable spot in the
Session, we can instead buffer provisional writes into dataflows accessible only
to the client that starts the transaction using e.g. temporary tables.

All writes could be rewritten as modifications to a temporary table; one for
inserts, one for deletes.

Reads from any table in a transaction that has had a write occur to it could be
rewritten to something that recomposes to:

```sql
SELECT * FROM original_table
UNION ALL SELECT * FROM insert_table
EXCEPT ALL SELECT * FROM delete_table;
```

For example:
```
materialize=> INSERT INTO original_table VALUES (1), (2), (2);
INSERT 0 2
materialize=> INSERT INTO insert_table VALUES (3), (4);
INSERT 0 1
materialize=> INSERT INTO delete_table VALUES (2), (4);
INSERT 0 1
materialize=> SELECT * FROM original_table
UNION ALL SELECT * FROM insert_table
EXCEPT ALL SELECT * FROM delete_table;
```
```
 a
---
1
2
3
(2 rows)
```

#### Implementation sketch

- `Session` must track whether it is currently in a transaction or not
- If a write occurs in a transaction, check if its source has its provisional
  components up and running and if not spin up some number of dataflows we can
  send our provisional values for:
  - Inserts
  - Deletes
  - A view reflecting the composite of the source table +
    inserts - deletes.

  All of the provisional components should be temporary to prevent other
  clients from seeing them, and also has the added benefit of automatically
  dropping them if the client disappears. They should have "impossible to
  guess" names, e.g. `format!("{}_{}_{}", table, uuid, operation)`.

  The provisional tables need to have the same schema as the original table.
  With access to the catalog's `RelationDesc`, this shouldn't be too tedious.

  If these resources exist, route the values to them as appropriate.
- All reads in a transaction need to check if the item being read from has a
  provisional composite view to read from and if so, perform its reads from that
  rather than the named table.

  There's no issue if we read from the original source for one operation and
  then a provisional source later on because we only read from the original
  source when we know we haven't modified it. If some other client has modified
  it since the beginning of our transaction, we'll detect that at commit time.

When committing, all of the datums in the insert and delete tables need to be
fed to the source table's dataflow, the only complexity of that would be
inverting the sign of the delete values' multiplicity.

After committing or aborting, the coord must drop all provisional objects.

### Prevent writes based on reads from having their read values change

We can check that no writes occurred to any table we read from outside of our
transaction iff we perform any writes. This makes our current read- and
write-only transactions performant, while letting us continue to expand our
semantics.

- Read-only transactions are all reads that occur as of a specific time in the
  past. Given that blended operation transactions change values at some point in
  the future, there is no concern that we mangle the linearizability guarantee
  here.
- Write-only transactions cannot be side-effecting because they do not rely on
  the presence or absence of any particular values within any table. As long as
  these are atomically applied at some timestamp, they are always linearizable.
- Blended operation transactions, though mean performing a read from a table
  means that we need to guarantee that the values from that table have not
  changed from the time the read occurs and the time the transaction's
  committed because we could have made a decision on how which values to use
  in some insert statement. This means that as we proceed through a
  transaction's statements, we need to aggregate all of the sources we have
  read from, and on committing, ensure that no writes (including deletes)
  occurred from the time we started our transaction until it's committed. If
  there have been we need to either retry the transaction or return an error
  to the client that we could not complete the transaction.

#### Implementation sketch
- `Session` must track whether it is currently in a transaction or not
- At beginning of transaction, copy all of the logical timestamps of all
  objects.
- During transaction, aggregate IDs of all permanent objects read from (i.e.
  when reading from provisional objects, collect the IDs of the objects they're
  based on).
- On commit, ensure that timestamps of read-from objects have not changed
  - If they have, error to the client that the operation failed but is retryable

Note that when selecting from rapidly updating sources, it's likely that you
could never complete a blended operation transaction for want of the continually
increasing timestamp. However, we could possibly still allow this with automatic
retries + stopping the world.

Retryable errors require more customer education because we need to ensure users
understand that their application code needs to sniff out errors to determine if
they should retry the transaction. Cockroach has a similar ergonomic concern,
which was largely mitigated with application code samples that perform this
check. Working on documentation there, these pages were well received.

#### Automatic retries

If we encounter a retryable error, we could perform the retry on the client's
behalf by stopping the world for the read-from resources, and re-executing all
of the transaction's operations. This would require caching the executed plans,
as well as ensuring everything can be stopped, e.g. source updates. Because at
this point the transaction is no longer interactive, the pause would be somewhat
bounded in length.

This seems incredibly intrusive but desirable behavior––so maybe not the
default, but permitted through something like:

```
BEGIN;
SET FORCE SUCCESS ON RETRYABLE ERROR;
...
```

Maybe we could develop this as a fast-follow feature after only allowing blended
operation transactions on tables to determine the scale of impact of pausing.

Without a mechanism like this, I don't immediately see how we could support
blended operation transactions on sources generally. An option then is to only
allow blended operation transactions to read from tables. This unblocks
Fivetran, at least, even if isn't quite as exciting as generalizable
transactions.

## Limitations

This approach will certainly not move us closer to being an OLTP database;
transactions' throughput will certainly be lower than it is now.

Memory usage will increase as we need to track a lot of state per transaction
(i.e. all frontiers).

Our conflict detection will only be at the granularity of tables, not individual
rows. This broad swath means retryable errors will be easy to manufacture
unless/until we support automatic retries. This will mean rearchitecting client
applications to handle retries, which Cockroach's users could do, but were not
used to (according to mjibson).

An alternative design here would be to track the filtering operations we ran and
perform some analysis if the rows in those filter sets are the same as the rows
we saw originally. That doesn't immediately decompose into a simple set
operation that I see, though, so not sure if that is practical whatsoever. In
most RDBMSes, this kind of detection is done using data adjacent to or intrinsic
to the row.
    
### Open questions
How/where do we track tables' logical timestamps? We need to check these on
commit to ensure that the table wasn't written to during our transaction.



# Graveyard







# Implementation proposal

The two large proposals here are:
- Within a transaction, perform writes on temporary tables that are built on top
  of the source tables (provisional write tracking); reads from those tables
  should then instead be from this manipulated data flow.
- When committing a transaction, ensure that no writes occurred to any table
  read from during the transaction.

## Provisional write tracking


## Linearizability
















## Proposed amendments to the current design

Buffer all rows that we would insert into the `Session`, and then send them to
the dataflow layer.


Right now, Materialize supports linearizability by strictly serializing all
operations in such a way that it is impossible for any writes to occur between a
read and a write influenced by that read (satisfying "Read then Write"
invariant), and guarantees that writes to dataflow only occur on a transaction's commit
 all writes only occur when they should be visible to ever

At the highest level, we instrument this guarantee by disallowing reads and
writes within the same transaction. What this means has some subtlety to it,
though.

- When starting a transaction (i.e. `BEGIN`), the first operation you perform
  defines the type of operations that are permitted for this transaction; they
  must be of a uniform type. Perform a read, you're in a read-only transaction
  and the same is true of writes. This guarantees you don't perform a write
  based on a read within a transaction. 


 We instrument this in two different
ways:

- Explicitly disallowing reads and writes within the same transaction. This
  means you simply cannot perform a read that influences a write within a
  transaction. This completely sidesteps the issue core to linearizability
  violations where you take an action based on some outdated view of the data.


 This kind of pessimistic concurrency
control happens, as you'd expect, through a lock; in our case a global write
lock that simply prevents any table from being written to when a read is
outstanding that can influenced a write.

Currently, though, this locking mechanism is scoped to only support operations
that are "easy" to convert to atomic operations: namely `UPDATE` and `DELETE`.




 a write influenced by a read, which is a fancy way of saying we don't allow mixing reads and
writes in the same transaction. Any transaction is either strictly all reads or
strictly all writes. How that provides linearizability is left as an exercise
for the reader, but if you can disprove this notion, please let us know.

This "separation of concerns" between reads and writes lets us do something very
neat––we simply cache all writes we want to commit on the `Session` struct, and
once we're ready to commit it, we––after ensuring we're permitted to write––blat
down all of the values we've been accumulating. When doing this, the coord
provides the session a global write lock to ensure that despite the asynchrony
of our operations, we have unfettered access to the data.

"Read then Write" operations (UPDATE + DELETE) are slightly different in that
they perform a read, and then immediately perform a write based on those
values––however, they cannot be part of a "transaction" in the BEGIN/COMMIT
sense, and must only be part of an "implicit transaction," which is a fancy way
of saying a single statement that immediately and automatically commits its
mutations. When performing a Read then Write, the Coordinator provides the lock
to the session for the duration of the operation to ensure no other session
manipulates the database between the read and the write, guaranteeing its
atomicity.
