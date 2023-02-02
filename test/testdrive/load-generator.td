# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

> SHOW SOURCES
accounts                subsource      <null>
auction_house           load-generator ${arg.default-storage-size}
auction_house_progress  subsource <null>
auctions                subsource      <null>
bids                    subsource      <null>
organizations           subsource      <null>
users                   subsource      <null>

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');

# Validate that the ID column of the load generator data is usable as a key.
> CREATE SINK accounts_sink FROM accounts
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-accounts-${testdrive.seed}')
  KEY (id)
  FORMAT JSON
  ENVELOPE UPSERT;

# Show that AST of subsource contains REFERENCES option
> SHOW CREATE SOURCE accounts
materialize.public.accounts "CREATE SUBSOURCE \"materialize\".\"public\".\"accounts\" (\"id\" \"pg_catalog\".\"int8\" NOT NULL, \"org_id\" \"pg_catalog\".\"int8\" NOT NULL, \"balance\" \"pg_catalog\".\"int8\" NOT NULL, UNIQUE (\"id\")) WITH (REFERENCES = true)"

# Query automatically generated progress topic
$ set-regex match=\d+ replacement=<NUMBER>
> SELECT count FROM auction_house_progress
<NUMBER>

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM auction_house_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper:             0 <>\n                          upper:[            0 <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.auction_house_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[            0 <>]\n"
