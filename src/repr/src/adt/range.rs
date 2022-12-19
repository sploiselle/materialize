// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{self, Debug, Display};

use bitflags::bitflags;
use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use num_traits::CheckedAdd;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::row::DatumNested;
use crate::scalar::DatumKind;
use crate::Datum;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.range.rs"));

bitflags! {
    pub(crate) struct Flags: u8 {
        const EMPTY = 1;
        const LB_INCLUSIVE = 1 << 1;
        const LB_INFINITE = 1 << 2;
        const UB_INCLUSIVE = 1 << 3;
        const UB_INFINITE = 1 << 4;
    }
}

/// A continuous set of domain values.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Range<'a> {
    /// None value represents empty range
    pub inner: Option<RangeInner<'a>>,
}

impl<'a> Display for Range<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner {
            None => f.write_str("empty"),
            Some(i) => Display::fmt(&i, f),
        }
    }
}

/// Trait alias for traits required for generic range function implementations.
pub trait RangeOps<'a>:
    Debug + Ord + PartialOrd + Eq + PartialEq + CheckedAdd + TryFrom<Datum<'a>> + Into<Datum<'a>>
{
    fn step() -> Option<Self>;

    fn unwrap_datum(d: Datum<'a>) -> Self {
        <Self>::try_from(d)
            .unwrap_or_else(|_| panic!("cannot take {} to {}", d, type_name::<Self>()))
    }
}

impl<'a> RangeOps<'a> for i32 {
    fn step() -> Option<i32> {
        Some(1)
    }
}

impl<'a> Range<'a> {
    #[allow(dead_code)]
    fn contains<T: RangeOps<'a>>(&self, elem: &T) -> bool {
        match self.inner {
            None => false,
            Some(inner) => inner.lower.satisfied_by(elem) && inner.upper.satisfied_by(elem),
        }
    }

    // - Infinite bounds are always exclusive
    // - Ranges are empty if lower == upper unless:
    //   - Range type does not have step and both bounds are inclusive
    // - If type has step:
    //  - Exclusive bounds are rewritten as inclusive ++
    pub fn canonicalize(
        mut lower: RangeLowerBound<Datum<'a>>,
        mut upper: RangeUpperBound<Datum<'a>>,
    ) -> Result<Option<(RangeLowerBound<Datum<'a>>, RangeUpperBound<Datum<'a>>)>, InvalidRangeError>
    {
        match (lower.bound, upper.bound) {
            (Some(l), Some(u)) => {
                assert_eq!(
                    DatumKind::from(l),
                    DatumKind::from(u),
                    "finite bounds must be of same type"
                );
                if l > u {
                    return Err(InvalidRangeError::MisorderedRangeBounds);
                }
            }
            _ => {}
        };

        lower.canonicalize();
        upper.canonicalize();

        // If (x,x], range is empty
        Ok(
            if !(lower.inclusive && upper.inclusive)
                && lower.bound >= upper.bound
                && lower.bound.is_some()
            {
                // emtpy range
                None
            } else {
                // No change
                Some((lower, upper))
            },
        )
    }
}

/// Holds the upper and lower `DatumRangeBound`s for non-empty ranges.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeInnerGeneric<B> {
    pub lower: RangeLowerBound<B>,
    pub upper: RangeUpperBound<B>,
}

pub type RangeInner<'a> = RangeInnerGeneric<DatumNested<'a>>;

impl<'a> Display for RangeInner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.lower.inclusive { "[" } else { "(" })?;
        Display::fmt(&self.lower, f)?;
        f.write_str(",")?;
        Display::fmt(&self.upper, f)?;
        f.write_str(if self.upper.inclusive { "]" } else { ")" })
    }
}

impl<'a> Ord for RangeInner<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lower
            .cmp(&other.lower)
            .then(self.upper.cmp(&other.upper))
    }
}

impl<'a> PartialOrd for RangeInner<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Represents a terminal point of a range.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeBound<B, const UPPER: bool = false> {
    pub inclusive: bool,
    /// None value represents an infinite bound.
    pub bound: Option<B>,
}

impl<'a, const UPPER: bool> Display for RangeBound<DatumNested<'a>, UPPER> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.bound {
            None => Ok(()),
            Some(bound) => Display::fmt(bound, f),
        }
    }
}

impl<'a, const UPPER: bool> Ord for RangeBound<DatumNested<'a>, UPPER> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = match self.bound.cmp(&other.bound) {
            Ordering::Equal => {
                if self.inclusive == other.inclusive {
                    Ordering::Equal
                } else if self.inclusive {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            o => o,
        };
        if UPPER {
            ordering.reverse()
        } else {
            ordering
        }
    }
}

impl<'a, const UPPER: bool> PartialOrd for RangeBound<DatumNested<'a>, UPPER> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A `RangeBound` that sorts correctly for use as a lower bound.
pub type RangeLowerBound<B> = RangeBound<B, false>;

/// A `RangeBound` that sorts correctly for use as an upper bound.
pub type RangeUpperBound<B> = RangeBound<B, true>;

impl<'a, const UPPER: bool> RangeBound<DatumNested<'a>, UPPER> {
    /// Determines where `elem` lies in relation to the range bound.
    ///
    /// # Panics
    /// - If `self.bound.datum()` is not convertible to `T`.
    fn elem_cmp<T: RangeOps<'a>>(&self, elem: &T) -> Ordering {
        match self.bound.map(|bound| <T>::unwrap_datum(bound.datum())) {
            None if UPPER => Ordering::Greater,
            None => Ordering::Less,
            Some(bound) => bound.cmp(elem),
        }
    }

    /// Does `elem` satisfy this bound?
    fn satisfied_by<T: RangeOps<'a>>(&self, elem: &T) -> bool {
        match self.elem_cmp(elem) {
            // Inclusive always satisfied with equality, regardless of upper or
            // lower.
            Ordering::Equal => self.inclusive,
            // Upper satisfied with values less than itself
            Ordering::Greater => UPPER,
            // Lower satisfied with values greater than itself
            Ordering::Less => !UPPER,
        }
    }
}

impl<'a, const UPPER: bool> RangeBound<Datum<'a>, UPPER> {
    /// Create a new `RangeBound` whose value is "infinite" (i.e. None) if `d ==
    /// Datum::Null`, otherwise finite (i.e. Some).
    pub fn new(d: Datum<'a>, inclusive: bool) -> RangeBound<Datum<'a>, UPPER> {
        RangeBound {
            inclusive,
            bound: match d {
                Datum::Null => None,
                o => Some(o),
            },
        }
    }

    fn canonicalize(&mut self) {
        match self.bound {
            None => {
                self.inclusive = false;
            }
            Some(value) => match value {
                d @ Datum::Int32(_) => self.canonicalize_inner::<i32>(d),
                _ => todo!(),
            },
        }
    }

    fn canonicalize_inner<T: RangeOps<'a>>(&mut self, d: Datum<'a>) {
        if let Some(step) = T::step() {
            // Upper bounds must be exclusive, lower bounds inclusive
            if UPPER == self.inclusive {
                let cur = <T>::unwrap_datum(d);
                self.bound = cur.checked_add(&step).map(|t| t.into());
                self.inclusive = !UPPER;
            }
        }
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum InvalidRangeError {
    MisorderedRangeBounds,
}

impl Display for InvalidRangeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InvalidRangeError::MisorderedRangeBounds => {
                f.write_str("range lower bound must be less than or equal to range upper bound")
            }
        }
    }
}

impl Error for InvalidRangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

// Required due to Proto decoding using string as its error type
impl From<InvalidRangeError> for String {
    fn from(e: InvalidRangeError) -> Self {
        e.to_string()
    }
}

impl RustType<ProtoInvalidRangeError> for InvalidRangeError {
    fn into_proto(&self) -> ProtoInvalidRangeError {
        use proto_invalid_range_error::*;
        use Kind::*;
        let kind = match self {
            InvalidRangeError::MisorderedRangeBounds => MisorderedRangeBounds(()),
        };
        ProtoInvalidRangeError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoInvalidRangeError) -> Result<Self, TryFromProtoError> {
        use proto_invalid_range_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                MisorderedRangeBounds(()) => Ok(InvalidRangeError::MisorderedRangeBounds),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoInvalidRangeError::kind`",
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Range;
    use crate::adt::range::{RangeLowerBound, RangeUpperBound};
    use crate::{Datum, RowArena};

    // TODO: Once SQL supports this, the test can be moved into SLT.
    #[test]
    fn test_range_contains() {
        fn test_range_contains_inner<'a>(
            range: Range,
            contains: Option<i32>,
            does_not_contain: Option<i32>,
        ) {
            if let Some(el) = contains {
                assert!(range.contains(&el), "el {:?}, range {}", el, range);
            }

            if let Some(el) = does_not_contain {
                assert!(!range.contains(&el), "el {:?}, range {:?}", el, range);
            }
        }

        let arena = RowArena::new();
        for (lower, lower_inclusive, upper, upper_inclusive, contains, does_not_contain) in [
            (
                Datum::Null,
                true,
                Datum::Int32(1),
                false,
                Some(i32::MIN),
                Some(1),
            ),
            (Datum::Null, true, Datum::Int32(1), true, Some(1), Some(2)),
            (Datum::Null, true, Datum::Null, false, Some(i32::MAX), None),
            (Datum::Null, true, Datum::Null, true, Some(i32::MAX), None),
            (
                Datum::Null,
                false,
                Datum::Int32(1),
                false,
                Some(i32::MIN),
                Some(1),
            ),
            (Datum::Null, false, Datum::Int32(1), true, Some(1), Some(2)),
            (Datum::Null, false, Datum::Null, false, Some(i32::MAX), None),
            (Datum::Null, false, Datum::Null, true, Some(i32::MAX), None),
            (
                Datum::Int32(-1),
                true,
                Datum::Int32(1),
                false,
                Some(-1),
                Some(1),
            ),
            (
                Datum::Int32(-1),
                true,
                Datum::Int32(1),
                true,
                Some(1),
                Some(-2),
            ),
            (
                Datum::Int32(-1),
                false,
                Datum::Int32(1),
                false,
                Some(0),
                Some(-1),
            ),
            (
                Datum::Int32(-1),
                false,
                Datum::Int32(1),
                true,
                Some(1),
                Some(-1),
            ),
            (Datum::Int32(1), true, Datum::Null, false, Some(1), Some(-1)),
            (
                Datum::Int32(1),
                true,
                Datum::Null,
                true,
                Some(i32::MAX),
                Some(-1),
            ),
            (Datum::Int32(1), false, Datum::Null, false, Some(2), Some(1)),
            (
                Datum::Int32(1),
                false,
                Datum::Null,
                true,
                Some(i32::MAX),
                Some(1),
            ),
        ] {
            let range = arena.make_datum(|packer| {
                packer
                    .push_range(
                        RangeLowerBound::new(lower, lower_inclusive),
                        RangeUpperBound::new(upper, upper_inclusive),
                    )
                    .unwrap();
            });

            let range = match range {
                Datum::Range(inner) => inner,
                _ => unreachable!(),
            };

            test_range_contains_inner(range, contains, does_not_contain);
        }

        let range = arena.make_datum(|packer| {
            packer.push_empty_range();
        });

        let range = match range {
            Datum::Range(inner) => inner,
            _ => unreachable!(),
        };

        for el in i16::MIN..i16::MAX {
            test_range_contains_inner(range, None, Some(el.into()));
        }
    }
}
