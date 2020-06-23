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
    an adversarial string
    "materialize"."public"."mz_data"
    ';

> ALTER SOURCE mz_data RENAME TO renamed_mz_data;

> ALTER VIEW mz_view RENAME TO renamed_mz_view;

> ALTER INDEX mz_view_primary_idx RENAME TO renamed_index;

> SHOW SOURCES;
SOURCES
-------
renamed_mz_data

> SHOW VIEWS
VIEWS
-----
byzantine_view
dependent_view
oppositional_view
renamed_mz_view

> SHOW CREATE VIEW renamed_mz_view
View                                Create View
---------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.renamed_mz_view  "CREATE VIEW \"materialize\".\"public\".\"renamed_mz_view\" AS SELECT * FROM \"materialize\".\"public\".\"renamed_mz_data\""

> SHOW INDEXES FROM renamed_mz_view
Source_or_view                      Key_name                          Column_name Expression Null  Seq_in_index
---------------------------------------------------------------------------------------------------------------
 materialize.public.renamed_mz_view materialize.public.renamed_index  a           <null>     false            1
 materialize.public.renamed_mz_view materialize.public.renamed_index  b           <null>     false            2
 materialize.public.renamed_mz_view materialize.public.renamed_index  mz_obj_no   <null>     false            3

> SHOW CREATE VIEW dependent_view
View                                Create View
------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.dependent_view   "CREATE VIEW \"materialize\".\"public\".\"dependent_view\" AS SELECT * FROM \"materialize\".\"public\".\"renamed_mz_view\""

! ALTER VIEW byzantine_view RENAME TO renamed_mz_view
renamed_mz_view is already taken by item in schema

# Creates alias to avoid ambiguity
> ALTER VIEW renamed_mz_view RENAME TO b

> ALTER VIEW b RENAME TO mz_view

> CREATE SCHEMA other

> CREATE VIEW other.t1 AS SELECT a FROM mz_view;
> CREATE VIEW t2 AS SELECT a, b FROM mz_view;
> CREATE VIEW cross_schema_view AS
  SELECT t1.a, t2.b
  FROM other.t1
  JOIN t2
  ON t1.a = t2.a;

> ALTER VIEW t2 RENAME TO t1;

> ALTER VIEW t1 RENAME TO t2;

# We cannot 
> CREATE VIEW t1_column_alias AS SELECT t1.a AS AS t1 FROM (
  SELECT t1.a, t2.b
  FROM other.t1
  JOIN t2
  ON t1.a = t2.a
);

# We also cannot rename t2 to t1 in this scenario because we don't know
# if we'd be changing the name of the output.
> CREATE VIEW t1_table_alias AS SELECT t1.a FROM (
  SELECT t1.a, t2.b
  FROM other.t1
  JOIN t2
  ON t1.a = t2.a
) AS t1;

! ALTER VIEW t2 RENAME TO t1;
unresolvable renaming conflict
