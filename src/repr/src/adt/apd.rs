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

/// The maximum number of digits expressable in an APD.
pub const APD_DATUM_WIDTH: usize = 13;
pub const APD_AGG_WIDTH: usize = 27;
pub const APD_MAX_PRECISION: usize = APD_DATUM_WIDTH * 3;

/// Parses am `i128` from a buffer storing the two's complement representation
/// of a set of bytes in big-endian byte order.
pub fn twos_complement_be_to_i128(input: &[u8]) -> Result<i128, anyhow::Error> {
    if input.len() > 16 {
        bail!("rdn exceeds maximum precision {}", APD_MAX_PRECISION);
    }
    let mut buf = [0; 16];
    for (i, digit) in input.iter().rev().enumerate() {
        buf[i] = *digit;
    }
    let mut significand = i128::from_le_bytes(buf);
    if !input.is_empty() && input.len() < 16 && ((input[0] & 0x80) != 0) {
        // This is tricky. In two's-complement representation, the weight of
        // the high order digit is negative. If we're filling out the entire
        // i128, then i128::from_le_bytes has already accounted for this.
        // Otherwise, if the high-order bit in this particular decimal is
        // set, we've incorrectly used it to contribute a positive weight.
        //
        // For example, consider the one-byte number 0xba. In one-byte two's
        // complement, this represents -70:
        //
        //     1(-2^7) + 0(2^6) + 1(2^5) + 1(2^4) + 1(2^3) + 0(2^2) + 1(2^1) + 0(2^0)
        //
        // We'll, however, have interpreted it as 186, because we'll have
        // assigned the highest bit a weight of 128, instead of -128. To
        // compensate, we subtract twice the weight of the highest digit--
        // in the example, 256.
        significand -= 2_i128.pow((input.len() * 8) as u32);
    }

    Ok(significand)
}

fn cx<const W: usize>() -> Context<Decimal<W>> {
    let mut cx = Context::<Decimal<W>>::default();
    cx.set_max_exponent(isize::try_from(W * 3).unwrap())
        .unwrap();
    cx.set_min_exponent(-(isize::try_from(W * 3).unwrap()))
        .unwrap();
    cx
}

/// Returns a new context approrpriate for operating on APD datums
pub fn cx_datum() -> Context<Decimal<APD_DATUM_WIDTH>> {
    cx::<APD_DATUM_WIDTH>()
}

/// Returns a new context approrpriate for operating on APD aggregations
pub fn cx_agg() -> Context<Decimal<APD_AGG_WIDTH>> {
    cx::<APD_AGG_WIDTH>()
}

/// Checks whether the context represents an over- or underflow w/r/t our
/// desired precision.
pub fn over_or_under_flow(cx: &Context<Decimal<APD_DATUM_WIDTH>>) -> bool {
    let s = cx.status();
    s.inexact() || s.rounded() || s.subnormal()
}

/// Returns `n`'s precision, i.e. the total number of digits represented by `n`
/// in standard notation.
pub fn get_precision(n: &OrderedDecimal<Decimal<APD_DATUM_WIDTH>>) -> u32 {
    let e = n.0.exponent();
    if e >= 0 {
        // Positive exponent
        n.0.digits() + u32::try_from(e).unwrap()
    } else {
        // Negative exponent
        let d = n.0.digits();
        let e = u32::try_from(e.abs()).unwrap();
        // Precision is...
        // - d if decimal point splits numbers
        // - e if e dominates number of digits
        std::cmp::max(d, e)
    }
}

/// Ensures that a generated `OrderedDecimal<Decimal>` does not exceed
/// `rust-dec`'s maximum precision. This method is useful when accepting input
/// from e.g. users, Kafka.
///
///  Errors if:
/// - `cx` has an `inexact` status.
/// - `n`'s total precision exceeds [`APD_MAX_PRECISION`].
pub fn check_max_precision_strict(
    n: &OrderedDecimal<Decimal<APD_DATUM_WIDTH>>,
) -> Result<(), anyhow::Error> {
    if exceeds_max_precision(&n) {
        bail!(
            "APD value {} exceed maximum precision {}",
            n,
            APD_MAX_PRECISION
        )
    } else {
        Ok(())
    }
}

/// Does `n`'s total precision exceed [`APD_MAX_PRECISION`]?
///
/// This method is useful for calculations that can acceptably return inexact
/// results, e.g. division.
pub fn exceeds_max_precision(n: &OrderedDecimal<Decimal<APD_DATUM_WIDTH>>) -> bool {
    get_precision(n) > APD_MAX_PRECISION as u32
}

// Returns `n`'s scale, i.e. the number of digits used after the decimal point.
pub fn get_scale(n: &OrderedDecimal<Decimal<APD_DATUM_WIDTH>>) -> u8 {
    u8::try_from(-n.0.exponent()).unwrap()
}

/// Rescale `n` as an `OrderedDecimal` with the described scale and return the
/// scale used.
pub fn rescale(
    n: &mut OrderedDecimal<Decimal<APD_DATUM_WIDTH>>,
    scale: u8,
) -> Result<(), anyhow::Error> {
    let mut cx = Context::<Decimal<APD_DATUM_WIDTH>>::default();
    cx.rescale(
        &mut n.0,
        &Decimal::<APD_DATUM_WIDTH>::from(-i32::from(scale)),
    );
    check_max_precision_strict(n)
}
