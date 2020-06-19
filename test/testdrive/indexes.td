# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": 1}
{"a": 2, "b": 1}
{"a": 3, "b": 1}
{"a": 1, "b": 2}

> CREATE SOURCE data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

# Cannot select from a non-materialized source
! SELECT * FROM data
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> CREATE VIEW data_view as SELECT * from data

# Cannot select from a non-materialized view
! SELECT * FROM data_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> CREATE MATERIALIZED VIEW test1 AS
  SELECT b, sum(a) FROM data GROUP BY b

# Materialized views are synonymous with having an index automatically created
> SHOW INDEXES FROM test1
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------
materialize.public.test1  materialize.public.test1_primary_idx  b            <null>      false             1

# You can select from materialized views through their indexes
> SELECT * FROM test1
b  sum
------
1  6
2  1

# "Materialize" a non-materialized view by creating a default index
> CREATE DEFAULT INDEX ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2

# This index is equivalent in structure as if it was materialized originally
> CREATE MATERIALIZED VIEW mz_data_view as SELECT * from data
Source_or_view                Key_name                                     Column_name  Expression  Null   Seq_in_index
-----------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.mz_data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.mz_data_view_primary_idx  b            <null>      false             2

# Materializing a view makes it selectable
> SELECT b, sum(a) FROM data_view GROUP BY b
b  sum
------
1  6
2  1

# IF NOT EXISTS prevents multiple default indexes from being addded
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2

# This is true whether the index was created through CREATE MATERIALIZED or not
> CREATE DEFAULT INDEX IF NOT EXISTS ON test1

> SHOW INDEXES FROM test_1
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------
materialize.public.test1  materialize.public.test1_primary_idx  b            <null>      false             1

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON test1

> SHOW INDEXES FROM test1
Source_or_view            Key_name                               Column_name  Expression  Null   Seq_in_index
-------------------------------------------------------------------------------------------------------------
materialize.public.test1  materialize.public.test1_primary_idx   b            <null>      false             1
materialize.public.test1  materialize.public.test1_primary_idx1  b            <null>      false             1

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_a_idx        a            <null>      false             1

# IF NOT EXISTS work for automatically named indexes
> CREATE INDEX IF NOT EXISTS ON data_view (a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_a_idx        a            <null>      false             1

# IF NOT EXISTS only checks names, not structure
> CREATE INDEX data_view_a_again ON data_view (a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_a_idx        a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_a_again      a            <null>      false             1

# Materialized sources are synonymous with having an index automatically created
> CREATE MATIERALIZED SOURCE mz_data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'
