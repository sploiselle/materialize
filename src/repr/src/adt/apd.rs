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

use anyhow::bail;
use dec::{Context, Decimal, OrderedDecimal};
use lazy_static::lazy_static;

/// The maximum number of digits expressable in an APD.
pub const APD_DATUM_WIDTH: usize = 13;
pub const APD_DATUM_MAX_PRECISION: usize = APD_DATUM_WIDTH * 3;
pub const APD_AGG_WIDTH: usize = 27;
pub const APD_AGG_MAX_PRECISION: usize = APD_DATUM_WIDTH * 3;

lazy_static! {
    static ref CX_DATUM: Context<Decimal<APD_DATUM_WIDTH>> = {
        let mut cx = Context::<Decimal<APD_DATUM_WIDTH>>::default();
        cx.set_max_exponent(isize::try_from(APD_DATUM_MAX_PRECISION - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_DATUM_MAX_PRECISION).unwrap()))
            .unwrap();
        cx
    };
    static ref CX_AGG: Context<Decimal<APD_AGG_WIDTH>> = {
        let mut cx = Context::<Decimal<APD_AGG_WIDTH>>::default();
        cx.set_max_exponent(isize::try_from(APD_AGG_MAX_PRECISION - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_AGG_MAX_PRECISION).unwrap()))
            .unwrap();
        cx
    };
}

/// Returns a new context approrpriate for operating on APD datums
pub fn cx_datum() -> Context<Decimal<APD_DATUM_WIDTH>> {
    CX_DATUM.clone()
}

/// Returns a new context approrpriate for operating on APD aggregation values.
pub fn cx_agg() -> Context<Decimal<APD_AGG_WIDTH>> {
    CX_AGG.clone()
}

pub fn get_precision<const N: usize>(n: &Decimal<N>) -> u32 {
    let e = n.exponent();
    if e >= 0 {
        // Positive exponent
        n.digits() + u32::try_from(e).unwrap()
    } else {
        // Negative exponent
        let d = n.digits();
        let e = u32::try_from(e.abs()).unwrap();
        // Precision is...
        // - d if decimal point splits numbers
        // - e if e dominates number of digits
        std::cmp::max(d, e)
    }
}

pub fn exceeds_max_precision<const N: usize>(n: &Decimal<N>) -> bool {
    get_precision(n) > u32::try_from(N).unwrap() * 3
}

/// Returns `n`'s scale, i.e. the number of digits used after the decimal point.
pub fn get_scale(n: &OrderedDecimal<Decimal<APD_DATUM_WIDTH>>) -> u8 {
    u8::try_from(-n.0.exponent()).unwrap()
}

/// Rescale `n` as an `OrderedDecimal` with the described scale and return the
/// scale used.
pub fn rescale<const N: usize>(n: &mut Decimal<N>, scale: u8) -> Result<(), anyhow::Error> {
    let mut cx = Context::<Decimal<N>>::default();
    cx.rescale(n, &Decimal::<N>::from(-i32::from(scale)));
    if exceeds_max_precision(&n) {
        bail!(
            "APD value {} exceed maximum precision {}",
            n,
            APD_DATUM_MAX_PRECISION
        )
    }

    Ok(())
}
