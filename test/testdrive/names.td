$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "string"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": "dog"}

# The following objects can be renamed:
# - Views
# - Sources
# - Sinks
# - Indexes
# With potential dependencies like so:
# - Sources: Indexes, Sinks, Views
# - Views: Indexes, Sinks, Views

# Create library of objects and verify names
> CREATE MATERIALIZED SOURCE mz_data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SHOW INDEXES FROM mz_data
Source_or_view                Key_name                              Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.mz_data  materialize.public.mz_data_primary_idx  a            <null>      false             1
materialize.public.mz_data  materialize.public.mz_data_primary_idx  b            <null>      false             2
materialize.public.mz_data  materialize.public.mz_data_primary_idx  mz_obj_no    <null>      false             3

> CREATE MATERIALIZED VIEW mz_view AS
    SELECT * FROM mz_data

> SHOW INDEXES FROM mz_view
Source_or_view                Key_name                              Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.mz_view  materialize.public.mz_view_primary_idx  a            <null>      false             1
materialize.public.mz_view  materialize.public.mz_view_primary_idx  b            <null>      false             2
materialize.public.mz_view  materialize.public.mz_view_primary_idx  mz_obj_no    <null>      false             3

> CREATE VIEW dependent_view AS
    SELECT * FROM mz_view;

> CREATE VIEW byzantine_view AS
    SELECT mz_data.a, mz_view.b FROM mz_data JOIN mz_view ON mz_data.a = mz_view.a;

> CREATE VIEW oppositional_view AS
    SELECT * FROM mz_view WHERE b = '
    an adversarial string to ensure you''re
    not just blindly updating strings
    "materialize"."public"."mz_data"
    ';

> ALTER SOURCE mz_data RENAME TO renamed_mz_data;

> ALTER VIEW mz_view RENAME TO renamed_mz_view;

> ALTER INDEX mz_view_primary_idx RENAME TO renamed_index;
