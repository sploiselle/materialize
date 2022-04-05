// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::GlobalId;

pub const TYPE_BOOL_GLOBAL_ID: GlobalId = GlobalId::System(1000);
pub const TYPE_BYTEA_GLOBAL_ID: GlobalId = GlobalId::System(1001);
pub const TYPE_INT8_GLOBAL_ID: GlobalId = GlobalId::System(1002);
pub const TYPE_INT4_GLOBAL_ID: GlobalId = GlobalId::System(1003);
pub const TYPE_TEXT_GLOBAL_ID: GlobalId = GlobalId::System(1004);
pub const TYPE_OID_GLOBAL_ID: GlobalId = GlobalId::System(1005);
pub const TYPE_FLOAT4_GLOBAL_ID: GlobalId = GlobalId::System(1006);
pub const TYPE_FLOAT8_GLOBAL_ID: GlobalId = GlobalId::System(1007);
pub const TYPE_BOOL_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1008);
pub const TYPE_BYTEA_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1009);
pub const TYPE_INT4_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1010);
pub const TYPE_TEXT_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1011);
pub const TYPE_INT8_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1012);
pub const TYPE_FLOAT4_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1013);
pub const TYPE_FLOAT8_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1014);
pub const TYPE_OID_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1015);
pub const TYPE_DATE_GLOBAL_ID: GlobalId = GlobalId::System(1016);
pub const TYPE_TIME_GLOBAL_ID: GlobalId = GlobalId::System(1017);
pub const TYPE_TIMESTAMP_GLOBAL_ID: GlobalId = GlobalId::System(1018);
pub const TYPE_TIMESTAMP_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1019);
pub const TYPE_DATE_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1020);
pub const TYPE_TIME_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1021);
pub const TYPE_TIMESTAMPTZ_GLOBAL_ID: GlobalId = GlobalId::System(1022);
pub const TYPE_TIMESTAMPTZ_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1023);
pub const TYPE_INTERVAL_GLOBAL_ID: GlobalId = GlobalId::System(1024);
pub const TYPE_INTERVAL_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1025);
pub const TYPE_NUMERIC_GLOBAL_ID: GlobalId = GlobalId::System(1026);
pub const TYPE_NUMERIC_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1027);
pub const TYPE_RECORD_GLOBAL_ID: GlobalId = GlobalId::System(1028);
pub const TYPE_RECORD_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1029);
pub const TYPE_UUID_GLOBAL_ID: GlobalId = GlobalId::System(1030);
pub const TYPE_UUID_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1031);
pub const TYPE_JSONB_GLOBAL_ID: GlobalId = GlobalId::System(1032);
pub const TYPE_JSONB_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1033);
pub const TYPE_ANY_GLOBAL_ID: GlobalId = GlobalId::System(1034);
pub const TYPE_ANYARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1035);
pub const TYPE_ANYELEMENT_GLOBAL_ID: GlobalId = GlobalId::System(1036);
pub const TYPE_ANYNONARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1037);
pub const TYPE_CHAR_GLOBAL_ID: GlobalId = GlobalId::System(1038);
pub const TYPE_VARCHAR_GLOBAL_ID: GlobalId = GlobalId::System(1039);
pub const TYPE_INT2_GLOBAL_ID: GlobalId = GlobalId::System(1040);
pub const TYPE_INT2_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1041);
pub const TYPE_BPCHAR_GLOBAL_ID: GlobalId = GlobalId::System(1042);
pub const TYPE_CHAR_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1043);
pub const TYPE_VARCHAR_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1044);
pub const TYPE_BPCHAR_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1045);
pub const TYPE_REGPROC_GLOBAL_ID: GlobalId = GlobalId::System(1046);
pub const TYPE_REGPROC_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1047);
pub const TYPE_REGTYPE_GLOBAL_ID: GlobalId = GlobalId::System(1048);
pub const TYPE_REGTYPE_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1049);
pub const TYPE_REGCLASS_GLOBAL_ID: GlobalId = GlobalId::System(1050);
pub const TYPE_REGCLASS_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1051);
pub const TYPE_INT2_VECTOR_GLOBAL_ID: GlobalId = GlobalId::System(1052);
pub const TYPE_INT2_VECTOR_ARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1053);
pub const TYPE_ANYCOMPATIBLE_GLOBAL_ID: GlobalId = GlobalId::System(1054);
pub const TYPE_ANYCOMPATIBLEARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1055);
pub const TYPE_ANYCOMPATIBLENONARRAY_GLOBAL_ID: GlobalId = GlobalId::System(1056);
pub const TYPE_LIST_GLOBAL_ID: GlobalId = GlobalId::System(1998);
pub const TYPE_MAP_GLOBAL_ID: GlobalId = GlobalId::System(1999);
pub const TYPE_ANYCOMPATIBLELIST_GLOBAL_ID: GlobalId = GlobalId::System(1997);
pub const TYPE_ANYCOMPATIBLEMAP_GLOBAL_ID: GlobalId = GlobalId::System(1996);
// Type IDs decrement
