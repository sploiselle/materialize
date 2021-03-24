// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions related to Materialize's numeric type, which is largely a wrapper
//! around [`rust-dec`].
//!
//! [`rust-dec`]: https://github.com/MaterializeInc/rust-dec/

use std::convert::TryFrom;

use dec::{Context, Decimal};
use lazy_static::lazy_static;

/// The maximum number of digits expressable in an APD.
pub const APD_DATUM_WIDTH: usize = 13;
pub const APD_MAX_PRECISION: usize = APD_DATUM_WIDTH * 3;

lazy_static! {
    static ref CX_DATUM: Context<Decimal<APD_DATUM_WIDTH>> = {
        let mut cx = Context::<Decimal<APD_DATUM_WIDTH>>::default();
        cx.set_max_exponent(isize::try_from(APD_DATUM_WIDTH * 3 - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_DATUM_WIDTH * 3).unwrap()))
            .unwrap();
        cx
    };
}

/// Returns a new context approrpriate for operating on APD datums.
pub fn cx_datum() -> Context<Decimal<APD_DATUM_WIDTH>> {
    CX_DATUM.clone()
}
