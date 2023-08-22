# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic connection functionality

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

# Create working connection + some data to ingest
$ kafka-create-topic topic=connection_test partitions=1
$ kafka-ingest format=bytes topic=connection_test
1,2

> CREATE CONNECTION conn TO KAFKA (BROKER '${testdrive.kafka-addr}')

> SELECT name, type from mz_connections
name       type
------------------------------
conn   kafka

> CREATE CONNECTION progress_override TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    PROGRESS TOPIC 'some_progress_topic'
  )

> CREATE SOURCE mz_source (first, second)
  FROM KAFKA CONNECTION conn (TOPIC 'testdrive-connection_test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> SELECT * FROM mz_source
first second
------------
1     2

! ALTER CONNECTION conn RESET broker;
contains:invalid ALTER CONNECTION: invalid CONNECTION: must set either BROKER or BROKERS

! ALTER CONNECTION conn SET broker = 'abcd' WITH (validate = true);
contains:Failed to resolve hostname

> ALTER CONNECTION conn SET broker = 'abcd' WITH (validate = false);

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

> SHOW CREATE CONNECTION conn
name   create_sql
---------------------------------
materialize.public.conn   "CREATE CONNECTION \"materialize\".\"public\".\"conn\" TO KAFKA (BROKER = 'abcd')"

$ kafka-ingest format=bytes topic=connection_test
2,3

> ALTER CONNECTION conn SET broker = '${testdrive.kafka-addr}' WITH (validate = true);

> SELECT * FROM mz_source
first second
------------
1     2
2     3

> SHOW CREATE CONNECTION conn
name   create_sql
---------------------------------
materialize.public.conn   "CREATE CONNECTION \"materialize\".\"public\".\"conn\" TO KAFKA (BROKER = '${testdrive.kafka-addr}')"

> ALTER CONNECTION conn SET BROKERS ['${testdrive.kafka-addr}']

> SHOW CREATE CONNECTION conn
name   create_sql
---------------------------------
materialize.public.conn   "CREATE CONNECTION \"materialize\".\"public\".\"conn\" TO KAFKA (BROKERS = ('${testdrive.kafka-addr}'))"

> ALTER CONNECTION conn SET broker = '${testdrive.kafka-addr}';

! ALTER CONNECTION conn SET progress topic = 'new';
contains:cannot ALTER KAFKA option PROGRESS TOPIC

! ALTER CONNECTION conn RESET;
contains:cannot ALTER KAFKA option PROGRESS TOPIC
