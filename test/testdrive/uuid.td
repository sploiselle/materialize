# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test exercises UUIDs at the boundary (e.g., by sending them
# through pgwire). Operations on UUIDs are more thoroughly tested in
# uuid.slt.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {
                "name": "u",
                "type": {
                  "type": "string",
                  "logicalType": "uuid"
                }
              }
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row":{"u": "16fd95b0-65b7-4249-9b66-1547cd95923d"}}}
{"before": null, "after": {"row":{"u": "b141698b-fb7f-492d-bc8a-0d159641c7a3"}}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=10
{"before": null, "after": null}

> CREATE SOURCE data FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW data_view as SELECT * from data

> SHOW COLUMNS FROM data_view
name   nullable  type
---------------------
u      false     uuid

> SELECT * FROM data_view
"16fd95b0-65b7-4249-9b66-1547cd95923d"
"b141698b-fb7f-492d-bc8a-0d159641c7a3"

> SELECT '85907cb9-ac9b-4e35-84b8-60dc69368aca'::uuid
"85907cb9-ac9b-4e35-84b8-60dc69368aca"

> SELECT '85907cb9-ac9b-4e35-84b8-60dc69368aca'::uuid::text
"85907cb9-ac9b-4e35-84b8-60dc69368aca"

> CREATE SINK uuid_sink_${testdrive.seed} FROM data_view
  INTO AVRO OCF '${testdrive.temp-dir}/uuid-sink.ocf'

$ avro-ocf-verify sink=materialize.public.uuid_sink_${testdrive.seed}
{"before": null, "after": {"row":{"u": "16fd95b0-65b7-4249-9b66-1547cd95923d"}}}
{"before": null, "after": {"row":{"u": "b141698b-fb7f-492d-bc8a-0d159641c7a3"}}}
