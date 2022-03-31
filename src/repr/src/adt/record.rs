// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions related to Materialize's `record`/composite types.

use std::error::Error;

use itertools::Itertools;

use crate::scalar::ScalarType;

#[derive(Debug)]
pub enum RecordCmpError<'a> {
    DifferentNumberOfColumns,
    DissimilarColumnTypes {
        position: usize,
        l: &'a ScalarType,
        r: &'a ScalarType,
    },
}

impl<'a> std::fmt::Display for RecordCmpError<'a> {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        panic!("RecordCmpError needs to be turned into a string in SQL planning to return proper names for ScalarTypes");
    }
}

impl<'a> Error for RecordCmpError<'a> {}

/// `record` comparisons require the same number of columns, and that the
/// `record`s' columns be of the same type. This is checked on each plan of the
/// comparison between `record`
pub fn record_cmp<'a>(l: &'a ScalarType, r: &'a ScalarType) -> Result<(), RecordCmpError<'a>> {
    let l_fields = l.unwrap_record_element_type();
    let r_fields = r.unwrap_record_element_type();
    if l_fields.len() != r_fields.len() {
        return Err(RecordCmpError::DifferentNumberOfColumns);
    }

    for (position, (l_scalar_type, r_scalar_type)) in
        l_fields.iter().zip_eq(r_fields.iter()).enumerate()
    {
        if !match (l_scalar_type, r_scalar_type) {
            (l @ ScalarType::Record { .. }, r @ ScalarType::Record { .. }) => {
                record_cmp(l, r).is_ok()
            }
            (l, r) => l.base_eq(r),
        } {
            return Err(RecordCmpError::DissimilarColumnTypes {
                position: position + 1,
                l: l_scalar_type,
                r: r_scalar_type,
            });
        }
    }

    Ok(())
}
