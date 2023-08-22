# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_default_connection_validation = true
ALTER SYSTEM SET enable_connection_validation_syntax = true

> CREATE SECRET pgpass AS 'postgres'

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    BROKER '${testdrive.kafka-addr}'
  )
contains:POSTGRES connections do not support BROKER values

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
  WITH (VALIDATE = true)
contains:password authentication failed for user "no_such_user"

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER no_such_user,
    PASSWORD SECRET pgpass
  )
  WITH (VALIDATE = false)

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

# We know this connection is invalid
! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;
contains:failed to connect to PostgreSQL database
detail:password authentication failed for user "no_such_user"

> ALTER CONNECTION pg SET user = postgres
    WITH (VALIDATE = true);

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM pk_table;
1 one
2 two

# Break connection
! ALTER CONNECTION pg SET port = 1 WITH (VALIDATE = true)
contains:Connection refused

> ALTER CONNECTION pg SET port = 1 WITH (VALIDATE = false)

> SHOW CREATE CONNECTION testconn
name   create_sql
---------------------------------
materialize.public.testconn   "CREATE CONNECTION \"materialize\".\"public\".\"testconn\" TO KAFKA (BROKER = '${testdrive.kafka-addr}')"

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'pk_table';
true

> ALTER CONNECTION pg RESET port WITH (VALIDATE = true)

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pk_table VALUES (3, 'three');

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
false

> SELECT status = 'stalled' FROM mz_internal.mz_source_statuses WHERE name = 'pk_table';
false

> SELECT * FROM pk_table;
1 one
2 two
3 three

> SHOW CREATE CONNECTION testconn
name   create_sql
---------------------------------
materialize.public.testconn   "CREATE CONNECTION \"materialize\".\"public\".\"testconn\" TO KAFKA (BROKER = '${testdrive.kafka-addr}')"


# Misc. connection tests

! ALTER CONNECTION pg SET database 'abcd' WITH (VALIDATE = true);
contains:database "abcd" does not exist

! ALTER CONNECTION pg DROP database;
contains:invalid ALTER CONNECTION: DATABASE option is required

! ALTER CONNECTION pg SET user 'abcd' WITH (VALIDATE = true);
contains:password authentication failed for user "abcd"

! ALTER CONNECTION pg DROP user;
contains:invalid ALTER CONNECTION: USER option is required

! ALTER CONNECTION pg SET host 'abcd' WITH (VALIDATE = true);
contains:error connecting to server

! ALTER CONNECTION pg RESET host;
contains:invalid ALTER CONNECTION: HOST option is required

! CREATE SECRET invalid_pgpass AS 'wrong'

! ALTER CONNECTION pg SET password secret invalid_pgpass WITH (VALIDATE = true);
contains:password authentication failed for user "postgres"

! ALTER CONNECTION pg RESET password;
contains:password missing

! ALTER CONNECTION pg SET broker = 'abcd';
contains:POSTGRES connections do not support BROKER values

! ALTER CONNECTION pg RESET broker;
contains:POSTGRES connections do not support BROKER VALUES
