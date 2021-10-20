---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "sql"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{< diagram "copy-from.svg" >}}

Field | Use
------|-----
_table_name_ | Copy values to this table.
**(**_column_...**)** | Correlate the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>Without a column list, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.

Supported `option` values:

Name | Permitted values| Default value | Description
-----|---------------|------------
`FORMAT` | `TEXT`, `CSV` | `TEXT` | Sets the input formatting method. For more information see [Text formatting](#text-formatting), [CSV formatting](#csv-formatting).
`DELIMITER` | Single-quoted one-byte character | Format-dependent | Overrides the format's default column delimiter.
`NULL` | Single-quoted strings | Format-dependent | Specifies the string that represents a _NULL_ value.

## Details

### Text formatting

As described in the **Text Format** section of [PostgreSQL's documentation][pg-copy-from].

### CSV formatting

As described in the **CSV Format** section of [PostgreSQL's documentation][pg-copy-from]
except that:

- Single-column rows containing quoted end-of-data markers (e.g. `"\."`) will be
  treated as end-of-data markers despite being quoted. In PostgreSQL, this data
  would be escaped and would not terminate the data processing.

- Quoted null strings will be parsed as nulls, despite being quoted. In
  PostgreSQL, this data would be escaped.

  To ensure proper null handling, we recommend specifying a unique string for
  null values, and ensuring it is never quoted.

- Unterminated quotes are allowed, i.e. they do not generate errors. In
  PostgreSQL, all open unescaped quotation punctuation must have a matching
  piece of unescaped quotation punctuation or it generates an error.

## Example

```sql
COPY t FROM STDIN WITH (DELIMITER '|')
```

```sql
COPY t FROM STDIN (FORMAT CSV)
```

```sql
COPY t FROM STDIN (DELIMITER '|')
```

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html
