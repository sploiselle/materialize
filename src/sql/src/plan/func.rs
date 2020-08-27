// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TBD: Currently, `sql::func` handles matching arguments to their respective
//! built-in functions (for most built-in functions, at least).

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;

use anyhow::bail;
use itertools::Itertools;
use lazy_static::lazy_static;

use ore::collections::CollectionExt;
use repr::{ColumnName, Datum, ScalarType};
use sql_parser::ast::{BinaryOperator, Expr, UnaryOperator};

use super::expr::{
    AggregateFunc, BinaryFunc, CoercibleScalarExpr, NullaryFunc, ScalarExpr, TableFunc, UnaryFunc,
    VariadicFunc,
};
use super::query::{self, ExprContext, QueryLifetime};
use super::typeconv::{self, rescale_decimal, CastTo, CoerceTo};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Mirrored from [PostgreSQL's `typcategory`][typcategory].
///
/// Note that Materialize also uses a number of pseudotypes when planning, but
/// we have yet to need to integrate them with `TypeCategory`.
///
/// [typcategory]:
/// https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
pub enum TypeCategory {
    Bool,
    DateTime,
    Numeric,
    Pseudo,
    String,
    Timespan,
    UserDefined,
}

impl TypeCategory {
    /// Extracted from PostgreSQL 9.6.
    /// ```ignore
    /// SELECT array_agg(typname), typcategory
    /// FROM pg_catalog.pg_type
    /// WHERE typname IN (
    ///  'bool', 'bytea', 'date', 'float4', 'float8', 'int4', 'int8', 'interval', 'jsonb',
    ///  'numeric', 'text', 'time', 'timestamp', 'timestamptz'
    /// )
    /// GROUP BY typcategory
    /// ORDER BY typcategory;
    /// ```
    fn from_type(typ: &ScalarType) -> Self {
        match typ {
            ScalarType::Bool => Self::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::List(_) => Self::UserDefined,
            ScalarType::Date
            | ScalarType::Time
            | ScalarType::Timestamp
            | ScalarType::TimestampTz => Self::DateTime,
            ScalarType::Decimal(..)
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int32
            | ScalarType::Int64 => Self::Numeric,
            ScalarType::Interval => Self::Timespan,
            ScalarType::String => Self::String,
            ScalarType::Record { .. } => Self::Pseudo,
        }
    }

    fn from_param(param: &ParamType) -> Self {
        match param {
            ParamType::Plain(t) => Self::from_type(t),
            ParamType::ListAny => Self::UserDefined,
            ParamType::Any
            | ParamType::StringAny
            | ParamType::JsonbAny
            | ParamType::ListElementAny
            | ParamType::NonListAny => Self::Pseudo,
        }
    }

    /// Extracted from PostgreSQL 9.6.
    /// ```ignore
    /// SELECT typcategory, typname, typispreferred
    /// FROM pg_catalog.pg_type
    /// WHERE typispreferred = true
    /// ORDER BY typcategory;
    /// ```
    fn preferred_type(&self) -> Option<ScalarType> {
        match self {
            Self::Bool => Some(ScalarType::Bool),
            Self::DateTime => Some(ScalarType::TimestampTz),
            Self::Numeric => Some(ScalarType::Float64),
            Self::String => Some(ScalarType::String),
            Self::Timespan => Some(ScalarType::Interval),
            Self::Pseudo | Self::UserDefined => None,
        }
    }
}

struct Operation<R>(
    Box<dyn Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, anyhow::Error> + Send + Sync>,
);

/// Describes a single function's implementation.
pub struct FuncImpl<R> {
    params: ParamList,
    op: Operation<R>,
}

impl<R> fmt::Debug for FuncImpl<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FuncImpl")
            .field("params", &self.params)
            .field("op", &"<omitted>")
            .finish()
    }
}

fn nullary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| {
        assert!(exprs.is_empty());
        f(ecx)
    }))
}

fn identity_op() -> Operation<ScalarExpr> {
    unary_op(|_ecx, e| Ok(e))
}

fn unary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, ScalarExpr) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| f(ecx, exprs.into_element())))
}

fn binary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, ScalarExpr, ScalarExpr) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| {
        assert_eq!(exprs.len(), 2);
        let mut exprs = exprs.into_iter();
        let left = exprs.next().unwrap();
        let right = exprs.next().unwrap();
        f(ecx, left, right)
    }))
}

fn variadic_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(f))
}

impl From<UnaryFunc> for Operation<ScalarExpr> {
    fn from(u: UnaryFunc) -> Operation<ScalarExpr> {
        unary_op(move |_ecx, e| Ok(e.call_unary(u.clone())))
    }
}

impl From<BinaryFunc> for Operation<ScalarExpr> {
    fn from(b: BinaryFunc) -> Operation<ScalarExpr> {
        binary_op(move |_ecx, left, right| Ok(left.call_binary(right, b.clone())))
    }
}

impl From<VariadicFunc> for Operation<ScalarExpr> {
    fn from(v: VariadicFunc) -> Operation<ScalarExpr> {
        variadic_op(move |_ecx, exprs| {
            Ok(ScalarExpr::CallVariadic {
                func: v.clone(),
                exprs,
            })
        })
    }
}

impl From<AggregateFunc> for Operation<(ScalarExpr, AggregateFunc)> {
    fn from(a: AggregateFunc) -> Operation<(ScalarExpr, AggregateFunc)> {
        unary_op(move |_ecx, e| Ok((e, a.clone())))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes possible types of function parameters.
///
/// Note that this is not exhaustive and will likely require additions.
pub enum ParamList {
    Exact(Vec<ParamType>),
    Repeat(Vec<ParamType>),
}

impl ParamList {
    /// Validates that the number of input elements are viable for this set of
    /// parameters.
    fn validate_arg_len(&self, input_len: usize) -> bool {
        match self {
            Self::Exact(p) => p.len() == input_len,
            Self::Repeat(p) => input_len % p.len() == 0 && input_len > 0,
        }
    }

    /// Matches a `&[ScalarType]` derived from the user's function argument
    /// against this `ParamList`'s permitted arguments.
    fn match_scalartypes(&self, types: &[&ScalarType]) -> bool {
        types
            .iter()
            .enumerate()
            .all(|(i, t)| self[i].accepts_type_directly(t))
    }
}

impl<'a> ParamList {
    fn iter(&'a self) -> ParamListIter<'a> {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a ParamList {
    type Item = &'a ParamType;
    type IntoIter = ParamListIter<'a>;

    // note that into_iter() is consuming self
    fn into_iter(self) -> Self::IntoIter {
        ParamListIter {
            param_list: self,
            index: 0,
        }
    }
}

#[derive(Debug)]
pub struct ParamListIter<'a> {
    param_list: &'a ParamList,
    index: usize,
}

impl<'a> Iterator for ParamListIter<'a> {
    type Item = &'a ParamType;
    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.param_list {
            ParamList::Exact(p) => {
                if self.index < p.len() {
                    Some(&p[self.index])
                } else {
                    None
                }
            }
            ParamList::Repeat(p) => Some(&p[self.index % p.len()]),
        };
        self.index += 1;
        result
    }
}

impl std::ops::Index<usize> for ParamList {
    type Output = ParamType;

    fn index(&self, i: usize) -> &Self::Output {
        match self {
            Self::Exact(p) => &p[i],
            Self::Repeat(p) => &p[i % p.len()],
        }
    }
}

/// Provides a shorthand function for writing `ParamList::Exact`.
impl From<Vec<ParamType>> for ParamList {
    fn from(p: Vec<ParamType>) -> ParamList {
        ParamList::Exact(p)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes parameter types; these are essentially just `ScalarType` with some
/// added flexibility.
pub enum ParamType {
    Plain(ScalarType),
    /// A psuedotype permitting any type.
    Any,
    /// A pseudotype permitting any type, but requires it to be cast to a
    /// `ScalarType::String`.
    StringAny,
    /// A pseudotype permitting any type, but requires it to be cast to a
    /// [`ScalarType::Jsonb`], or an element within a `Jsonb`.
    JsonbAny,
    /// A pseudotype permitting a `ScalarType::List` of any element type.
    ListAny,
    /// A pseudotype permitting all types, with more limitations than `Any`.
    ///
    /// These limitations include:
    /// - If multiple parameters expect `ListElementAny`, they must all be of
    ///   the same type.
    /// - If `ListElementAny` is used with `ListAny`, `ListElementAny`'s type
    ///   must be `ListAny`'s elements' type.
    ListElementAny,
    /// A pseudotype permitting any type except `ScalarType::List`.
    ///
    /// `NonListAny` is only used for concatenating text with other, non-list
    /// types.
    NonListAny,
}

impl ParamType {
    /// Does `self` accept arguments of type `t` without casting?
    fn accepts_type_directly(&self, t: &ScalarType) -> bool {
        use ParamType::*;
        match self {
            Plain(s) => *s == t.desaturate(),
            Any | StringAny | JsonbAny | ListElementAny => true,
            // Accepting string is necessary for PG compatibility, even though
            // we don't support a string-like list constructor.
            ListAny => t.is_list() || *t == ScalarType::String,
            NonListAny => !t.is_list(),
        }
    }

    /// Does `self` accept arguments of type `t` with an implicitly allowed cast?
    fn accepts_type_implicitly(&self, t: &ScalarType) -> bool {
        use ParamType::*;
        match self {
            Plain(s) => typeconv::get_cast(t, &CastTo::Implicit(s.clone())).is_some(),
            Any | JsonbAny | StringAny | ListElementAny => true,
            ListAny => t.is_list() || *t == ScalarType::String,
            NonListAny => !t.is_list(),
        }
    }

    /// Does `self` accept arguments of category `c`?
    fn accepts_cat(&self, c: &TypeCategory) -> bool {
        use ParamType::*;
        match self {
            Plain(_) => TypeCategory::from_param(&self) == *c,
            ListAny => TypeCategory::from_param(&self) == *c || TypeCategory::String == *c,
            Any | StringAny | JsonbAny | ListElementAny | NonListAny => true,
        }
    }

    /// Does `t`'s [`TypeCategory`] prefer `self`? This question can make
    /// more sense with the understanding that pseudotypes are never preferred.
    fn is_preferred_by(&self, t: &ScalarType) -> bool {
        if let Some(pt) = TypeCategory::from_type(t).preferred_type() {
            *self == pt
        } else {
            false
        }
    }

    /// Is `self` the preferred parameter type for its `TypeCategory`?
    fn prefers_self(&self) -> bool {
        if let Some(pt) = TypeCategory::from_param(self).preferred_type() {
            *self == pt
        } else {
            false
        }
    }

    /// Returns the [`CoerceTo`] value appropriate to coerce arguments to
    /// types compatible with `self`.
    fn get_coerce_to(&self) -> CoerceTo {
        use ParamType::*;
        use ScalarType::*;
        match self {
            Plain(s) => CoerceTo::Plain(s.clone()),
            Any | StringAny | ListElementAny | NonListAny => CoerceTo::Plain(String),
            JsonbAny => CoerceTo::JsonbAny,
            ListAny => CoerceTo::Plain(List(Box::new(String))),
        }
    }

    /// Determines which, if any, [`CastTo`] value is appropriate to cast
    /// `arg_type` to a [`ScalarType`] compatible with `self`.
    fn get_cast_to_for_type(&self, arg_type: &ScalarType) -> Result<CastTo, anyhow::Error> {
        use ParamType::*;
        use ScalarType::*;
        Ok(match self {
            Plain(Decimal(..)) if matches!(arg_type, Decimal(..)) => {
                CastTo::Implicit(arg_type.clone())
            }
            Plain(s) => CastTo::Implicit(s.clone()),
            // Reflexive cast because `self` accepts any type.
            Any | ListElementAny => CastTo::Implicit(arg_type.clone()),
            JsonbAny => CastTo::JsonbAny,
            StringAny => CastTo::Explicit(ScalarType::String),
            // `NonListAny` is only used to convert non-list elements to text
            // for concatenation.
            NonListAny if !arg_type.is_list() => CastTo::Explicit(ScalarType::String),
            // Reflexive cast for any `ScalarType::List` because any list type is
            // valid, but only list types are valid.
            ListAny if arg_type.is_list() => CastTo::Implicit(arg_type.clone()),
            _ => bail!(
                "arguments cannot be implicitly cast to any implementation's parameters; \
                 try providing explicit casts"
            ),
        })
    }
}

impl PartialEq<ScalarType> for ParamType {
    fn eq(&self, other: &ScalarType) -> bool {
        use ParamType::*;
        match self {
            Plain(s) => *s == other.desaturate(),
            ListAny => other.is_list(),
            Any | StringAny | JsonbAny | ListElementAny | NonListAny => false,
        }
    }
}

impl PartialEq<ParamType> for ScalarType {
    fn eq(&self, other: &ParamType) -> bool {
        other == self
    }
}

impl From<ScalarType> for ParamType {
    fn from(s: ScalarType) -> ParamType {
        ParamType::Plain(s)
    }
}

#[derive(Clone)]
/// Tracks candidate implementations.
pub struct Candidate<'a, R> {
    /// The implementation under consideration.
    fimpl: &'a FuncImpl<R>,
    candidate_exprs: Vec<ScalarExpr>,
    exact_matches: usize,
    preferred_types: usize,
}

impl<'a, R> fmt::Debug for Candidate<'a, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Candidate")
            .field("fimpl", &self.fimpl)
            .field("candidate_exprs", &self.candidate_exprs)
            .field("exact_matches", &self.exact_matches)
            .field("preferred_types", &self.preferred_types)
            .finish()
    }
}

#[derive(Clone, Debug)]
/// Determines best implementation to use given some user-provided arguments.
/// For more detail, see `ArgImplementationMatcher::select_implementation`.
pub struct ArgImplementationMatcher<'a> {
    ident: &'a str,
    ecx: &'a ExprContext<'a>,
}

impl<'a> ArgImplementationMatcher<'a> {
    /// Selects the best implementation given the provided `args` using a
    /// process similar to [PostgreSQL's parser][pgparser], and returns the
    /// `ScalarExpr` to invoke that function.
    ///
    /// # Errors
    /// - When the provided arguments are not valid for any implementation, e.g.
    ///   cannot be converted to the appropriate types.
    /// - When all implementations are equally valid.
    ///
    /// [pgparser]: https://www.postgresql.org/docs/current/typeconv-oper.html
    pub fn select_implementation<R>(
        ident: &'a str,
        err_string_gen: fn(&str, &[Option<ScalarType>], String) -> String,
        ecx: &'a ExprContext<'a>,
        impls: &[FuncImpl<R>],
        cexprs: Vec<CoercibleScalarExpr>,
    ) -> Result<R, anyhow::Error> {
        let m = Self { ident, ecx };

        let types: Vec<_> = cexprs
            .iter()
            .map(|e| ecx.column_type(e).map(|t| t.scalar_type))
            .collect();
        // Remove all `impls` we know are invalid.
        let l = cexprs.len();
        let impls = impls
            .iter()
            .filter(|i| i.params.validate_arg_len(l))
            .collect();

        // try-catch in Rust.
        match || -> Result<R, anyhow::Error> {
            let c = m.find_match(&cexprs, &types, impls)?;
            (c.fimpl.op.0)(ecx, c.candidate_exprs)
        }() {
            Ok(s) => Ok(s),
            Err(e) => bail!(err_string_gen(ident, &types, e.to_string())),
        }
    }

    /// Finds an exact match based on the arguments, or, if no exact match,
    /// finds the best match available. Patterned after [PostgreSQL's type
    /// conversion matching algorithm][pgparser].
    ///
    /// Inline prefixed with number are taken from the "Function Type
    /// Resolution" section of the aforelinked page.
    ///
    /// [pgparser]: https://www.postgresql.org/docs/current/typeconv-func.html
    fn find_match<'b, R>(
        &self,
        cexprs: &[CoercibleScalarExpr],
        types: &[Option<ScalarType>],
        impls: Vec<&'b FuncImpl<R>>,
    ) -> Result<Candidate<'b, R>, anyhow::Error> {
        let all_types_known = types.iter().all(|t| t.is_some());

        // Check for exact match.
        if all_types_known {
            let known_types: Vec<_> = types.iter().filter_map(|t| t.as_ref()).collect();

            let matching_impls: Vec<&FuncImpl<_>> = impls
                .iter()
                .filter(|i| i.params.match_scalartypes(&known_types))
                .cloned()
                .collect();

            if matching_impls.len() == 1 {
                return Ok(Candidate {
                    fimpl: &matching_impls[0],
                    candidate_exprs: self
                        .resolve_polymorphic_types(&matching_impls[0].params, cexprs)?,
                    exact_matches: types.len(),
                    preferred_types: 0,
                });
            }
        }

        // No exact match. Apply PostgreSQL's best match algorithm.
        // Generate candidates by assessing their compatibility with each
        // implementation's parameters.
        let mut candidates: Vec<Candidate<_>> = Vec::new();
        macro_rules! maybe_get_last_candidate {
            () => {
                if candidates.len() == 1 {
                    return Ok(candidates.remove(0));
                }
            };
        }
        let mut max_exact_matches = 0;
        for fimpl in impls {
            let mut valid_candidate = true;
            let mut exact_matches = 0;
            let mut preferred_types = 0;

            for (i, arg_type) in types.iter().enumerate() {
                let param_type = &fimpl.params[i];

                match arg_type {
                    Some(arg_type) if param_type == arg_type => {
                        exact_matches += 1;
                    }
                    Some(arg_type) => {
                        if !param_type.accepts_type_implicitly(arg_type) {
                            valid_candidate = false;
                            break;
                        }
                        if param_type.is_preferred_by(arg_type) {
                            preferred_types += 1;
                        }
                    }
                    None => {
                        if param_type.prefers_self() {
                            preferred_types += 1;
                        }
                    }
                }
            }

            let candidate_exprs = match self.resolve_polymorphic_types(&fimpl.params, cexprs) {
                Ok(c) => c,
                Err(_) => continue,
            };
            println!("candidate exprs {:?}", candidate_exprs);
            // 4.a. Discard candidate functions for which the input types do not match
            // and cannot be converted (using an implicit conversion) to match.
            // unknown literals are assumed to be convertible to anything for this
            // purpose.
            if valid_candidate {
                max_exact_matches = std::cmp::max(max_exact_matches, exact_matches);
                candidates.push(Candidate {
                    fimpl,
                    candidate_exprs,
                    exact_matches,
                    preferred_types,
                });
                // println!("candidates {:?}\n\n\n", candidates);
            }
        }

        for c in &candidates {
            println!("{:?}", c);
        }

        println!("types {:?}", types);

        if candidates.is_empty() {
            bail!(
                "arguments cannot be implicitly cast to any implementation's parameters; \
                 try providing explicit casts"
            )
        }

        maybe_get_last_candidate!();

        // 4.c. Run through all candidates and keep those with the most exact matches on
        // input types. Keep all candidates if none have exact matches.
        candidates.retain(|c| c.exact_matches >= max_exact_matches);

        maybe_get_last_candidate!();

        // 4.d. Run through all candidates and keep those that accept preferred types
        // (of the input data type's type category) at the most positions where
        // type conversion will be required.
        let mut max_preferred_types = 0;
        for c in &candidates {
            max_preferred_types = std::cmp::max(max_preferred_types, c.preferred_types);
        }
        candidates.retain(|c| c.preferred_types >= max_preferred_types);

        maybe_get_last_candidate!();

        if all_types_known {
            bail!(
                "unable to determine which implementation to use; try providing \
                 explicit casts to match parameter types"
            )
        }

        println!("Into the unknown");

        let mut found_known = false;
        let mut types_match = true;
        let mut common_type: Option<ScalarType> = None;

        for (i, arg_type) in types.iter().enumerate() {
            let mut selected_category: Option<TypeCategory> = None;
            let mut found_string_candidate = false;
            let mut categories_match = true;

            match arg_type {
                // 4.e. If any input arguments are unknown, check the type categories accepted
                // at those argument positions by the remaining candidates.
                None => {
                    for c in candidates.iter() {
                        // 4.e. cont: At each  position, select the string category if
                        // any candidate accepts that category. (This bias
                        // towards string is appropriate since an
                        // unknown-type literal looks like a string.)
                        if c.fimpl.params[i].accepts_type_directly(&ScalarType::String) {
                            found_string_candidate = true;
                            selected_category = Some(TypeCategory::String);
                            break;
                        }
                        // 4.e. cont: Otherwise, if all the remaining candidates accept
                        // the same type category, select that category.
                        let this_category = TypeCategory::from_param(&c.fimpl.params[i]);
                        println!(
                            "this cat {:?}, selected cat {:?}\n\n",
                            this_category, selected_category
                        );
                        match (&selected_category, &this_category) {
                            (Some(selected_category), this_category) => {
                                categories_match =
                                    *selected_category == *this_category && categories_match
                            }
                            (None, this_category) => {
                                selected_category = Some(this_category.clone())
                            }
                        }
                    }

                    println!("selected_category {:?}", selected_category);

                    // 4.e. cont: Otherwise fail because the correct choice cannot be
                    // deduced without more clues.
                    // (ed: this doesn't mean fail entirely, simply moving onto 4.f)
                    if !found_string_candidate && !categories_match {
                        println!("breaking");
                        break;
                    }

                    // 4.e. cont: Now discard candidates that do not accept the selected
                    // type category. Furthermore, if any candidate accepts a
                    // preferred type in that category, discard candidates that
                    // accept non-preferred types for that argument.
                    let selected_category = selected_category.unwrap();

                    let preferred_type = selected_category.preferred_type();
                    let mut found_preferred_type_candidate = false;
                    candidates.retain(|c| {
                        if let Some(typ) = &preferred_type {
                            found_preferred_type_candidate = c.fimpl.params[i]
                                .accepts_type_directly(typ)
                                || found_preferred_type_candidate;
                        }
                        c.fimpl.params[i].accepts_cat(&selected_category)
                    });

                    if found_preferred_type_candidate {
                        let preferred_type = preferred_type.unwrap();
                        candidates
                            .retain(|c| c.fimpl.params[i].accepts_type_directly(&preferred_type));
                    }
                }
                Some(typ) => {
                    found_known = true;
                    // Track if all known types are of the same type; use this info in 4.f.
                    if let Some(common_type) = &common_type {
                        types_match = types_match && *common_type == *typ
                    } else {
                        common_type = Some(typ.clone());
                    }
                }
            }
        }

        maybe_get_last_candidate!();

        // 4.f. If there are both unknown and known-type arguments, and all the
        // known-type arguments have the same type, assume that the unknown
        // arguments are also of that type, and check which candidates can
        // accept that type at the unknown-argument positions.
        // (ed: We know unknown argument exists if we're in this part of the code.)
        if found_known && types_match {
            let common_type = common_type.unwrap();
            println!("common_type {:?}", common_type);
            for (i, raw_arg_type) in types.iter().enumerate() {
                if raw_arg_type.is_none() {
                    candidates.retain(|c| c.fimpl.params[i].accepts_type_directly(&common_type));
                }
            }

            maybe_get_last_candidate!();
        }

        bail!(
            "unable to determine which implementation to use; try providing \
             explicit casts to match parameter types"
        )
    }

    /// Implements constraints for PostgreSQL polymorphic types.
    fn resolve_polymorphic_types(
        &self,
        params: &ParamList,
        cexprs: &[CoercibleScalarExpr],
    ) -> Result<Vec<ScalarExpr>, anyhow::Error> {
        let mut types = Vec::new();
        let mut args = Vec::new();
        // 1. Coerce all exprs to the parameters' types; if that fails, this param list is invalid.
        for (p, c) in params.iter().zip(cexprs.iter()) {
            let coerce_to = p.get_coerce_to();
            let arg = match typeconv::plan_coerce(self.ecx, c.clone(), coerce_to.clone()) {
                Ok(a) => a,
                Err(e) => {
                    println!("Cannot coerce {:?} to {:?}", c, coerce_to);
                    return Err(e);
                }
            };
            let arg_type = self.ecx.scalar_type(&arg);
            args.push(arg);
            types.push(arg_type);
        }

        // 2. With coerced exprs and their types, determine the appropriate polymorphic types.
        println!(
            "resolving polymorphic types \n\t params {:?} \n\t types {:?}",
            params, types
        );
        let constrained_params = if params.iter().any(|p| match p {
            ParamType::ListAny | ParamType::ListElementAny => true,
            _ => false,
        }) {
            println!("Inside the polymorphic resolution");
            let mut constrained_type: Option<ScalarType> = None;
            // Determine any known type among `ListAny` or `ListElementAny` parameters.
            for (p, t) in params.iter().zip(types.iter()) {
                match (p, t) {
                    (ParamType::ListAny, ScalarType::List(typ)) => {
                        constrained_type = Some((**typ).clone());
                        break;
                    }
                    (ParamType::ListElementAny, typ) | (ParamType::NonListAny, typ) => {
                        constrained_type = Some(typ.clone());
                        break;
                    }
                    _ => {}
                }
            }

            let constrained_type = constrained_type.unwrap_or(ScalarType::String);
            println!("constrained type {:?}", constrained_type);

            // Regenerate Option<ScalarTypep> to coerce to constrained type.
            let types: Vec<_> = cexprs
                .iter()
                .map(|e| self.ecx.column_type(e).map(|t| t.scalar_type))
                .collect();
            let mut param_types = Vec::new();
            // Constrain polymorphic types.
            for (p, t) in params.iter().zip(types.iter()) {
                match (p, t) {
                    (ParamType::ListAny, Some(ScalarType::List(typ))) => {
                        if constrained_type != **typ {
                            bail!(
                                "incompatible types; have List({}), can only accept List({})",
                                typ,
                                constrained_type
                            );
                        }
                        param_types.push(ParamType::Plain(ScalarType::List(typ.clone())));
                    }
                    (ParamType::ListAny, None) => {
                        param_types.push(ParamType::Plain(ScalarType::List(Box::new(
                            constrained_type.clone(),
                        ))));
                    }
                    (ParamType::ListElementAny, Some(typ)) => {
                        if &constrained_type != typ {
                            bail!(
                                "incompatible types; have {}, can only accept {}",
                                typ,
                                constrained_type
                            );
                        }
                        param_types.push(ParamType::Plain(typ.clone()));
                    }
                    (ParamType::ListElementAny, None) => {
                        param_types.push(ParamType::Plain(constrained_type.clone()));
                    }
                    (ParamType::NonListAny, Some(typ)) => {
                        if matches!(constrained_type, ScalarType::List(..)) {
                            bail!("incompatible types; cannot accept {}", constrained_type)
                        } else if &constrained_type != typ {
                            bail!(
                                "incompatible types; have {}, can only accept {}",
                                typ,
                                constrained_type
                            );
                        }
                        param_types.push(ParamType::Plain(typ.clone()));
                    }
                    (ParamType::NonListAny, None) => {
                        if matches!(constrained_type, ScalarType::List(..)) {
                            bail!("incompatible types; cannot accept {}", constrained_type)
                        }
                        param_types.push(ParamType::Plain(constrained_type.clone()));
                    }
                    _ => param_types.push(p.clone()),
                }
            }
            println!("Resolved polymorphic types {:?}", param_types);
            match params {
                ParamList::Exact(_) => ParamList::Exact(param_types),
                ParamList::Repeat(_) => ParamList::Repeat(param_types),
            }
        } else {
            params.clone()
        };

        println!("Trying to restrain down to {:?}", constrained_params);
        constrained_params
            .iter()
            .zip(cexprs.iter())
            .map(|(p, a)| self.coerce_arg_to_type(a.clone(), p))
            .collect()
    }

    /// Generates `ScalarExpr` necessary to coerce `Expr` into the `ScalarType`
    /// corresponding to `ParameterType`; errors if not possible. This can only
    /// work within the `func` module because it relies on `ParameterType`.
    fn coerce_arg_to_type(
        &self,
        arg: CoercibleScalarExpr,
        param_type: &ParamType,
    ) -> Result<ScalarExpr, anyhow::Error> {
        let coerce_to = param_type.get_coerce_to();
        let arg = typeconv::plan_coerce(self.ecx, arg, coerce_to)?;
        let arg_type = self.ecx.scalar_type(&arg);
        let cast_to = param_type.get_cast_to_for_type(&arg_type)?;
        match typeconv::plan_cast(self.ident, self.ecx, arg.clone(), cast_to.clone()) {
            Ok(o) => {
                println!("plan_cast succeeded with {:?}, {:?}", arg, cast_to);
                Ok(o)
            }
            Err(e) => {
                println!("plan_cast failed with {:?}, {:?}", arg, cast_to);
                Err(e)
            }
        }
    }
}

macro_rules! concat_err {
    ($l:expr, $r:expr) => {
        bail!("Cannot concatenate {} and {}", $l, $r)
    };
}

/// Provides shorthand for converting `Vec<ScalarType>` into `Vec<ParamType>`.
macro_rules! params {
    (($($p:expr),*)...) => { ParamList::Repeat(vec![$($p.into(),)*]) };
    ($($p:expr),*)      => { ParamList::Exact(vec![$($p.into(),)*]) };
}

/// Provides shorthand for inserting [`FuncImpl`]s into arbitrary `HashMap`s.
macro_rules! insert_impl {
    ($hashmap:ident, $key:expr, $($params:expr => $op:expr),+) => {
        let impls = vec![
            $(FuncImpl {
                params: $params.into(),
                op: $op.into(),
            },)+
        ];

        $hashmap.entry($key).or_default().extend(impls);
    };
}

/// Provides a macro to write HashMap "literals" for matching some key to
/// `Vec<FuncImpl>`.
macro_rules! impls {
    {
        $(
            $name:expr => {
                $($params:expr => $op:expr),+
            }
        ),+
    } => {{
        let mut m: HashMap<_, Vec<FuncImpl<_>>> = HashMap::new();
        $(
            insert_impl!(m, $name, $($params => $op),+);
        )+
        m
    }};
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_IMPLS: HashMap<&'static str, Vec<FuncImpl<ScalarExpr>>> = {
        use ParamType::*;
        use ScalarType::*;
        impls! {
            "abs" => {
                params!(Int32) => UnaryFunc::AbsInt32,
                params!(Int64) => UnaryFunc::AbsInt64,
                params!(Decimal(0, 0)) => UnaryFunc::AbsDecimal,
                params!(Float32) => UnaryFunc::AbsFloat32,
                params!(Float64) => UnaryFunc::AbsFloat64
            },
            "ascii" => {
                params!(String) => UnaryFunc::Ascii
            },
            "btrim" => {
                params!(String) => UnaryFunc::TrimWhitespace,
                params!(String, String) => BinaryFunc::Trim
            },
            "bit_length" => {
                params!(Bytes) => UnaryFunc::BitLengthBytes,
                params!(String) => UnaryFunc::BitLengthString
            },
            "ceil" => {
                params!(Float32) => UnaryFunc::CeilFloat32,
                params!(Float64) => UnaryFunc::CeilFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::CeilDecimal(s)))
                })
            },
            "char_length" => {
                params!(String) => UnaryFunc::CharLength
            },
            "concat" => {
                 params!((StringAny)...) => variadic_op(|_ecx, mut exprs| {
                    // Unlike all other `StringAny` casts, `concat` uses an
                    // implicit behavior for converting bools to strings.
                    for e in &mut exprs {
                        if let ScalarExpr::CallUnary {
                            func: func @ UnaryFunc::CastBoolToStringExplicit,
                            ..
                        } = e {
                            *func = UnaryFunc::CastBoolToStringImplicit;
                        }
                    }
                    Ok(ScalarExpr::CallVariadic { func: VariadicFunc::Concat, exprs })
                })
            },
            "convert_from" => {
                params!(Bytes, String) => BinaryFunc::ConvertFrom
            },
            "current_timestamp" => {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "current_timestamp"))
            },
            "date_part" => {
                params!(String, Interval) => BinaryFunc::DatePartInterval,
                params!(String, Timestamp) => BinaryFunc::DatePartTimestamp,
                params!(String, TimestampTz) => BinaryFunc::DatePartTimestampTz
            },
            "date_trunc" => {
                params!(String, Timestamp) => BinaryFunc::DateTruncTimestamp,
                params!(String, TimestampTz) => BinaryFunc::DateTruncTimestampTz
            },
            "floor" => {
                params!(Float32) => UnaryFunc::FloorFloat32,
                params!(Float64) => UnaryFunc::FloorFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::FloorDecimal(s)))
                })
            },
            "internal_avg_promotion" => {
                // Promotes a numeric type to the smallest fractional type that
                // can represent it. This is primarily useful for the avg
                // aggregate function, so that the avg of an integer column does
                // not get truncated to an integer, which would be surprising to
                // users (#549).
                params!(Float32) => identity_op(),
                params!(Float64) => identity_op(),
                params!(Decimal(0, 0)) => identity_op(),
                params!(Int32) => unary_op(|ecx, e| {
                      super::typeconv::plan_cast(
                          "internal.avg_promotion", ecx, e,
                          CastTo::Explicit(ScalarType::Decimal(10, 0)),
                      )
                })
            },
            "jsonb_array_length" => {
                params!(Jsonb) => UnaryFunc::JsonbArrayLength
            },
            "jsonb_build_array" => {
                params!() => VariadicFunc::JsonbBuildArray,
                params!((JsonbAny)...) => VariadicFunc::JsonbBuildArray
            },
            "jsonb_build_object" => {
                params!() => VariadicFunc::JsonbBuildObject,
                params!((StringAny, JsonbAny)...) =>
                    VariadicFunc::JsonbBuildObject
            },
            "jsonb_pretty" => {
                params!(Jsonb) => UnaryFunc::JsonbPretty
            },
            "jsonb_strip_nulls" => {
                params!(Jsonb) => UnaryFunc::JsonbStripNulls
            },
            "jsonb_typeof" => {
                params!(Jsonb) => UnaryFunc::JsonbTypeof
            },
            "length" => {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::CharLength,
                params!(Bytes, String) => BinaryFunc::EncodedBytesCharLength
            },
            "octet_length" => {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::ByteLengthString
            },
            "list_append" => {
                vec![ListAny, ListElementAny] => binary_op(|ecx, lhs, rhs| {
                    let ltyp = ecx.scalar_type(&lhs);
                    let rtyp = ecx.scalar_type(&rhs);
                    if *ltyp.unwrap_list_element_type() != rtyp {
                        concat_err!(ltyp, rtyp)
                    }
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListElementConcat))
                })
            },
            "list_cat" => {
                vec![ListAny, ListAny] => binary_op(|ecx, lhs, rhs| {
                    let ltyp = ecx.scalar_type(&lhs);
                    let rtyp = ecx.scalar_type(&rhs);
                    if ltyp != rtyp {
                        concat_err!(ltyp, rtyp)
                    }
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListListConcat))

                })
            },
            "list_ndims" => {
                vec![ListAny] => unary_op(|ecx, e| {
                    ecx.require_experimental_mode("list_ndims")?;
                    let d = ecx.scalar_type(&e).unwrap_list_n_dims();
                    Ok(ScalarExpr::literal(Datum::Int32(d as i32), ScalarType::Int32))
                })
            },
            "list_length" => {
                vec![ListAny] => UnaryFunc::ListLength
            },
            "list_length_max" => {
                vec![ListAny, Plain(Int64)] => binary_op(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("list_length_max")?;
                    let max_dim = ecx.scalar_type(&lhs).unwrap_list_n_dims();
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListLengthMax{ max_dim }))
                })
            },
            "list_prepend" => {
                vec![ListElementAny, ListAny] => binary_op(|ecx, lhs, rhs| {
                    let ltyp = ecx.scalar_type(&lhs);
                    let rtyp = ecx.scalar_type(&rhs);
                    if ltyp != *rtyp.unwrap_list_element_type() {
                        concat_err!(ltyp, rtyp)
                    }
                    Ok(lhs.call_binary(rhs, BinaryFunc::ElementListConcat))
                })
            },
            "ltrim" => {
                params!(String) => UnaryFunc::TrimLeadingWhitespace,
                params!(String, String) => BinaryFunc::TrimLeading
            },
            "make_timestamp" => {
                params!(Int64, Int64, Int64, Int64, Int64, Float64) => VariadicFunc::MakeTimestamp
            },
            "mz_logical_timestamp" => {
                params!() => nullary_op(|ecx| {
                    match ecx.qcx.lifetime {
                        QueryLifetime::OneShot => {
                            Ok(ScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp))
                        }
                        QueryLifetime::Static => bail!("mz_logical_timestamp cannot be used in static queries"),
                    }
                })
            },
            "now" => {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "now"))
            },
            "replace" => {
                params!(String, String, String) => VariadicFunc::Replace
            },
            "round" => {
                params!(Float32) => UnaryFunc::RoundFloat32,
                params!(Float64) => UnaryFunc::RoundFloat64,
                params!(Decimal(0,0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::RoundDecimal(s)))
                }),
                params!(Decimal(0,0), Int64) => binary_op(|ecx, lhs, rhs| {
                    let (_, s) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    Ok(lhs.call_binary(rhs, BinaryFunc::RoundDecimal(s)))
                })
            },
            "rtrim" => {
                params!(String) => UnaryFunc::TrimTrailingWhitespace,
                params!(String, String) => BinaryFunc::TrimTrailing
            },
            "split_part" => {
                params!(String, String, Int64) => VariadicFunc::SplitPart
            },
            "substr" => {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "substring" => {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "sqrt" => {
                params!(Float32) => UnaryFunc::SqrtFloat32,
                params!(Float64) => UnaryFunc::SqrtFloat64,
                params!(Decimal(0,0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::SqrtDec(s)))
                })
            },
            "to_char" => {
                params!(Timestamp, String) => BinaryFunc::ToCharTimestamp,
                params!(TimestampTz, String) => BinaryFunc::ToCharTimestampTz
            },
            // > Returns the value as json or jsonb. Arrays and composites
            // > are converted (recursively) to arrays and objects;
            // > otherwise, if there is a cast from the type to json, the
            // > cast function will be used to perform the conversion;
            // > otherwise, a scalar value is produced. For any scalar type
            // > other than a number, a Boolean, or a null value, the text
            // > representation will be used, in such a fashion that it is a
            // > valid json or jsonb value.
            //
            // https://www.postgresql.org/docs/current/functions-json.html
            "to_jsonb" => {
                params!(JsonbAny) => identity_op()
            },
            "to_timestamp" => {
                params!(Float64) => UnaryFunc::ToTimestamp
            }
        }
    };
}

fn plan_current_timestamp(ecx: &ExprContext, name: &str) -> Result<ScalarExpr, anyhow::Error> {
    match ecx.qcx.lifetime {
        QueryLifetime::OneShot => Ok(ScalarExpr::literal(
            Datum::from(ecx.qcx.scx.pcx.wall_time),
            ScalarType::TimestampTz,
        )),
        QueryLifetime::Static => bail!("{} cannot be used in static queries", name),
    }
}

fn stringify_opt_scalartype(t: &Option<ScalarType>) -> String {
    match t {
        Some(t) => t.to_string(),
        None => "unknown".to_string(),
    }
}

fn func_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "Cannot call function {}({}): {}",
        ident,
        types.iter().map(|o| stringify_opt_scalartype(o)).join(", "),
        hint,
    )
}

/// Gets a built-in scalar function and the `ScalarExpr`s required to invoke it.
pub fn select_scalar_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<ScalarExpr, anyhow::Error> {
    let impls = match BUILTIN_IMPLS.get(ident) {
        Some(i) => i,
        None => bail!("function \"{}\" does not exist", ident),
    };

    let mut cexprs = Vec::new();
    for arg in args {
        let cexpr = query::plan_expr(ecx, arg)?;
        cexprs.push(cexpr);
    }

    ArgImplementationMatcher::select_implementation(ident, func_err_string, ecx, impls, cexprs)
}

lazy_static! {
    /// Correlates a `BinaryOperator` with all of its implementations.
    static ref BINARY_OP_IMPLS: HashMap<BinaryOperator, Vec<FuncImpl<ScalarExpr>>> = {
        use ScalarType::*;
        use BinaryOperator::*;
        use BinaryFunc::*;
        use ParamType::*;
        let mut m = impls! {
            // ARITHMETIC
            Plus => {
                params!(Int32, Int32) => AddInt32,
                params!(Int64, Int64) => AddInt64,
                params!(Float32, Float32) => AddFloat32,
                params!(Float64, Float64) => AddFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, AddDecimal))
                    })
                },
                params!(Interval, Interval) => AddInterval,
                params!(Timestamp, Interval) => AddTimestampInterval,
                params!(Interval, Timestamp) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampInterval)))
                },
                params!(TimestampTz, Interval) => AddTimestampTzInterval,
                params!(Interval, TimestampTz) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampTzInterval)))
                },
                params!(Date, Interval) => AddDateInterval,
                params!(Interval, Date) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateInterval)))
                },
                params!(Date, Time) => AddDateTime,
                params!(Time, Date) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateTime)))
                },
                params!(Time, Interval) => AddTimeInterval,
                params!(Interval, Time) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimeInterval)))
                }
            },
            Minus => {
                params!(Int32, Int32) => SubInt32,
                params!(Int64, Int64) => SubInt64,
                params!(Float32, Float32) => SubFloat32,
                params!(Float64, Float64) => SubFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                    Ok(lexpr.call_binary(rexpr, SubDecimal))
                }),
                params!(Interval, Interval) => SubInterval,
                params!(Timestamp, Timestamp) => SubTimestamp,
                params!(TimestampTz, TimestampTz) => SubTimestampTz,
                params!(Timestamp, Interval) => SubTimestampInterval,
                params!(TimestampTz, Interval) => SubTimestampTzInterval,
                params!(Date, Date) => SubDate,
                params!(Date, Interval) => SubDateInterval,
                params!(Time, Time) => SubTime,
                params!(Time, Interval) => SubTimeInterval,
                params!(Jsonb, Int64) => JsonbDeleteInt64,
                params!(Jsonb, String) => JsonbDeleteString
                // TODO(jamii) there should be corresponding overloads for
                // Array(Int64) and Array(String)
            },
            Multiply => {
                params!(Int32, Int32) => MulInt32,
                params!(Int64, Int64) => MulInt64,
                params!(Float32, Float32) => MulFloat32,
                params!(Float64, Float64) => MulFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    use std::cmp::*;
                    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
                    let so = max(max(min(s1 + s2, 12), s1), s2);
                    let si = s1 + s2;
                    let expr = lhs.call_binary(rhs, MulDecimal);
                    Ok(rescale_decimal(expr, si, so))
                })
            },
            Divide => {
                params!(Int32, Int32) => DivInt32,
                params!(Int64, Int64) => DivInt64,
                params!(Float32, Float32) => DivFloat32,
                params!(Float64, Float64) => DivFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    use std::cmp::*;
                    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
                    // Pretend all 0-scale numerators were of the same scale as
                    // their denominators for improved accuracy.
                    let s1_mod = if s1 == 0 { s2 } else { s1 };
                    let s = max(min(12, s1_mod + 6), s1_mod);
                    let si = max(s + 1, s2);
                    let lhs = rescale_decimal(lhs, s1, si);
                    let expr = lhs.call_binary(rhs, DivDecimal);
                    Ok(rescale_decimal(expr, si - s2, s))
                })
            },
            Modulus => {
                params!(Int32, Int32) => ModInt32,
                params!(Int64, Int64) => ModInt64,
                params!(Float32, Float32) => ModFloat32,
                params!(Float64, Float64) => ModFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                    Ok(lexpr.call_binary(rexpr, ModDecimal))
                })
            },

            // BOOLEAN OPS
            BinaryOperator::And => {
                params!(Bool, Bool) => BinaryFunc::And
            },
            BinaryOperator::Or => {
                params!(Bool, Bool) => BinaryFunc::Or
            },

            // LIKE
            Like => {
                params!(String, String) => MatchLikePattern
            },
            NotLike => {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchLikePattern)
                        .call_unary(UnaryFunc::Not))
                })
            },

            // REGEX
            RegexMatch => {
                params!(String, String) => MatchRegex { case_insensitive: false }
            },
            RegexIMatch => {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(rhs, MatchRegex { case_insensitive: true }))
                })
            },
            RegexNotMatch => {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchRegex { case_insensitive: false })
                        .call_unary(UnaryFunc::Not))
                })
            },
            RegexNotIMatch => {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchRegex { case_insensitive: true })
                        .call_unary(UnaryFunc::Not))
                })
            },

            // CONCAT
            Concat => {
                vec![Plain(String), NonListAny] => TextConcat,
                vec![NonListAny, Plain(String)] => TextConcat,
                params!(String, String) => TextConcat,
                params!(Jsonb, Jsonb) => JsonbConcat,
                params!(ListAny, ListAny) => binary_op(|ecx, lhs, rhs| {
                    Ok(lhs.call_binary(rhs, ListListConcat))
                    // let ltyp = ecx.scalar_type(&lhs);
                    // let rtyp = ecx.scalar_type(&rhs);

                    // if ltyp == rtyp {
                    // } else if ltyp == *rtyp.unwrap_list_element_type() {
                    //     Ok(lhs.call_binary(rhs, ElementListConcat))
                    // } else if *ltyp.unwrap_list_element_type() == rtyp {
                    //     Ok(lhs.call_binary(rhs, ListElementConcat))
                    // } else {
                    //     concat_err!(ltyp, rtyp)
                    // }
                }),
                params!(ListAny, ListElementAny) => binary_op(|ecx, lhs, rhs| {
                    let ltyp = ecx.scalar_type(&lhs);
                    let rtyp = ecx.scalar_type(&rhs);

                    if *ltyp.unwrap_list_element_type() != rtyp {
                        concat_err!(ltyp, rtyp)
                    }
                    Ok(lhs.call_binary(rhs, ListElementConcat))
                }),
                params!(ListElementAny, ListAny) => binary_op(|ecx, lhs, rhs| {
                    let ltyp = ecx.scalar_type(&lhs);
                    let rtyp = ecx.scalar_type(&rhs);

                    if ltyp != *rtyp.unwrap_list_element_type() {
                        concat_err!(ltyp, rtyp)
                    }
                    Ok(lhs.call_binary(rhs, ElementListConcat))
                })
            },

            //JSON
            JsonGet => {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: false },
                params!(Jsonb, String) => JsonbGetString { stringify: false }
            },
            JsonGetAsText => {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: true },
                params!(Jsonb, String) => JsonbGetString { stringify: true }
            },
            JsonContainsJson => {
                params!(Jsonb, Jsonb) => JsonbContainsJsonb,
                params!(Jsonb, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(
                        rhs.call_unary(UnaryFunc::CastStringToJsonb),
                        JsonbContainsJsonb,
                    ))
                }),
                params!(String, Jsonb) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(rhs, JsonbContainsJsonb))
                })
            },
            JsonContainedInJson => {
                params!(Jsonb, Jsonb) =>  binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs,
                        JsonbContainsJsonb
                    ))
                }),
                params!(Jsonb, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(lhs, BinaryFunc::JsonbContainsJsonb))
                }),
                params!(String, Jsonb) => binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs.call_unary(UnaryFunc::CastStringToJsonb),
                        BinaryFunc::JsonbContainsJsonb,
                    ))
                })
            },
            JsonContainsField => {
                params!(Jsonb, String) => JsonbContainsString
            },
            // COMPARISON OPS
            // n.b. Decimal impls are separated from other types because they
            // require a function pointer, which you cannot dynamically generate.
            BinaryOperator::Lt => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lt))
                    })
                }
            },
            BinaryOperator::LtEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lte))
                    })
                }
            },
            BinaryOperator::Gt => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gt))
                    })
                }
            },
            BinaryOperator::GtEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gte))
                    })
                }
            },
            BinaryOperator::Eq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Eq))
                    })
                }
            },
            BinaryOperator::NotEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::NotEq))
                    })
                }
            }
        };

        for (op, func) in vec![
            (BinaryOperator::Lt, BinaryFunc::Lt),
            (BinaryOperator::LtEq, BinaryFunc::Lte),
            (BinaryOperator::Gt, BinaryFunc::Gt),
            (BinaryOperator::GtEq, BinaryFunc::Gte),
            (BinaryOperator::Eq, BinaryFunc::Eq),
            (BinaryOperator::NotEq, BinaryFunc::NotEq)
        ] {
            insert_impl!(m, op,
                params!(Bool, Bool) => func.clone(),
                params!(Int32, Int32) => func.clone(),
                params!(Int64, Int64) => func.clone(),
                params!(Float32, Float32) => func.clone(),
                params!(Float64, Float64) => func.clone(),
                params!(Date, Date) => func.clone(),
                params!(Time, Time) => func.clone(),
                params!(Timestamp, Timestamp) => func.clone(),
                params!(TimestampTz, TimestampTz) => func.clone(),
                params!(Interval, Interval) => func.clone(),
                params!(Bytes, Bytes) => func.clone(),
                params!(String, String) => func.clone(),
                params!(Jsonb, Jsonb) => func.clone()
            );
        }

        m
    };
}

/// Rescales two decimals to have the same scale.
fn rescale_decimals_to_same(
    ecx: &ExprContext,
    lhs: ScalarExpr,
    rhs: ScalarExpr,
) -> (ScalarExpr, ScalarExpr) {
    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
    let so = std::cmp::max(s1, s2);
    let lexpr = rescale_decimal(lhs, s1, so);
    let rexpr = rescale_decimal(rhs, s2, so);
    (lexpr, rexpr)
}

fn binary_op_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "no overload for {} {} {}: {}",
        stringify_opt_scalartype(&types[0]),
        ident,
        stringify_opt_scalartype(&types[1]),
        hint,
    )
}

/// Plans a function compatible with the `BinaryOperator`.
pub fn plan_binary_op<'a>(
    ecx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, anyhow::Error> {
    let impls = match BINARY_OP_IMPLS.get(&op) {
        Some(i) => i,
        // TODO: these require sql arrays
        // JsonContainsAnyFields
        // JsonContainsAllFields
        // TODO: these require json paths
        // JsonGetPath
        // JsonGetPathAsText
        // JsonDeletePath
        // JsonContainsPath
        // JsonApplyPathPredicate
        None => unsupported!(op),
    };

    let mut cexprs = vec![query::plan_expr(ecx, left)?, query::plan_expr(ecx, right)?];

    ArgImplementationMatcher::select_implementation(
        &op.to_string(),
        binary_op_err_string,
        ecx,
        impls,
        cexprs,
    )
}

lazy_static! {
    /// Correlates a `UnaryOperator` with all of its implementations.
    static ref UNARY_OP_IMPLS: HashMap<UnaryOperator, Vec<FuncImpl<ScalarExpr>>> = {
        use ParamType::*;
        use ScalarType::*;
        use UnaryOperator::*;
        impls! {
            Not => {
                params!(Bool) => UnaryFunc::Not
            },

            Plus => {
                params!(Any) => identity_op()
            },

            Minus => {
                params!(Int32) => UnaryFunc::NegInt32,
                params!(Int64) => UnaryFunc::NegInt64,
                params!(Float32) => UnaryFunc::NegFloat32,
                params!(Float64) => UnaryFunc::NegFloat64,
                params!(ScalarType::Decimal(0, 0)) => UnaryFunc::NegDecimal,
                params!(Interval) => UnaryFunc::NegInterval
            }
        }
    };
}

fn unary_op_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "no overload for {} {}: {}",
        ident,
        stringify_opt_scalartype(&types[0]),
        hint,
    )
}

/// Plans a function compatible with the `UnaryOperator`.
pub fn plan_unary_op<'a>(
    ecx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, anyhow::Error> {
    let impls = match UNARY_OP_IMPLS.get(&op) {
        Some(i) => i,
        None => unsupported!(op),
    };

    let cexpr = vec![query::plan_expr(ecx, expr)?];

    ArgImplementationMatcher::select_implementation(
        &op.to_string(),
        unary_op_err_string,
        ecx,
        impls,
        cexpr,
    )
}

pub struct TableFuncPlan {
    pub func: TableFunc,
    pub exprs: Vec<ScalarExpr>,
    pub column_names: Vec<Option<ColumnName>>,
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_TABLE_IMPLS: HashMap<&'static str, Vec<FuncImpl<TableFuncPlan>>> = {
        use ScalarType::*;
        impls! {
            "csv_extract" => {
                params!(Int64, String) => binary_op(move |_ecx, ncols, input| {
                    let ncols = match ncols.into_literal_int64() {
                        None | Some(i64::MIN..=0) => {
                            bail!("csv_extract number of columns must be a positive integer literal");
                        },
                        Some(ncols) => ncols,
                    };
                    let ncols = usize::try_from(ncols).expect("known to be greater than zero");
                    Ok(TableFuncPlan {
                        func: TableFunc::CsvExtract(ncols),
                        exprs: vec![input],
                        column_names: (1..=ncols).map(|i| Some(format!("column{}", i).into())).collect(),
                    })
                })
            },
            "generate_series" => {
                params!(Int32, Int32) => binary_op(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt32,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                }),
                params!(Int64, Int64) => binary_op(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt64,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                })
            },
            "jsonb_array_elements" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_array_elements_text" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_each" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_each_text" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_object_keys" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbObjectKeys,
                        exprs: vec![jsonb],
                        column_names: vec![Some("jsonb_object_keys".into())],
                    })
                })
            },
            "regexp_extract" => {
                params!(String, String) => binary_op(move |_ecx, regex, haystack| {
                    let regex = match regex.into_literal_string() {
                        None => bail!("regex_extract requires a string literal as its first argument"),
                        Some(regex) => expr::AnalyzedRegex::new(&regex)?,
                    };
                    let column_names = regex
                        .capture_groups_iter()
                        .map(|cg| {
                            let name = cg.name.clone().unwrap_or_else(|| format!("column{}", cg.index));
                            Some(name.into())
                        })
                        .collect();
                    Ok(TableFuncPlan {
                        func: TableFunc::RegexpExtract(regex),
                        exprs: vec![haystack],
                        column_names,
                    })
                })
            },
            "repeat" => {
                params!(Int64) => unary_op(move |ecx, n| {
                    ecx.require_experimental_mode("repeat")?;
                    Ok(TableFuncPlan {
                        func: TableFunc::Repeat,
                        exprs: vec![n],
                        column_names: vec![]
                    })
                })
            }
        }
    };
}

pub fn is_table_func(ident: &str) -> bool {
    BUILTIN_TABLE_IMPLS.get(ident).is_some()
}

/// Plans a built-in table function.
pub fn select_table_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<TableFuncPlan, anyhow::Error> {
    if ident == "values" {
        // Produce a nice error message for the common typo
        // `SELECT * FROM VALUES (1)`.
        bail!("VALUES expression in FROM clause must be surrounded by parentheses");
    }

    let impls = match BUILTIN_TABLE_IMPLS.get(ident) {
        Some(i) => i,
        None => bail!("function \"{}\" does not exist", ident),
    };

    let mut cexprs = Vec::new();
    for arg in args {
        let cexpr = query::plan_expr(ecx, arg)?;
        cexprs.push(cexpr);
    }

    ArgImplementationMatcher::select_implementation(ident, func_err_string, ecx, impls, cexprs)
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_AGGREGATE_IMPLS: HashMap<&'static str, Vec<FuncImpl<(ScalarExpr, AggregateFunc)>>> = {
        use ParamType::*;
        use ScalarType::*;
        impls! {
            "array_agg" => {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("array_agg"))
            },
            "bool_and" => {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("bool_and"))
            },
            "bool_or" => {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("bool_or"))
            },
            "concat_agg" => {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("concat_agg"))
            },
            "count" => {
                params!() => nullary_op(|_ecx| {
                    // COUNT(*) is equivalent to COUNT(true).
                    Ok((ScalarExpr::literal_true(), AggregateFunc::Count))
                }),
                params!(Any) => AggregateFunc::Count
            },
            "internal_all" => {
                params!(Any) => AggregateFunc::All
            },
            "internal_any" => {
                params!(Any) => AggregateFunc::Any
            },
            "max" => {
                params!(Int32) => AggregateFunc::MaxInt32,
                params!(Int64) => AggregateFunc::MaxInt64,
                params!(Float32) => AggregateFunc::MaxFloat32,
                params!(Float64) => AggregateFunc::MaxFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::MaxDecimal,
                params!(Bool) => AggregateFunc::MaxBool,
                params!(String) => AggregateFunc::MaxString,
                params!(Date) => AggregateFunc::MaxDate,
                params!(Timestamp) => AggregateFunc::MaxTimestamp,
                params!(TimestampTz) => AggregateFunc::MaxTimestampTz
            },
            "min" => {
                params!(Int32) => AggregateFunc::MinInt32,
                params!(Int64) => AggregateFunc::MinInt64,
                params!(Float32) => AggregateFunc::MinFloat32,
                params!(Float64) => AggregateFunc::MinFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::MinDecimal,
                params!(Bool) => AggregateFunc::MinBool,
                params!(String) => AggregateFunc::MinString,
                params!(Date) => AggregateFunc::MinDate,
                params!(Timestamp) => AggregateFunc::MinTimestamp,
                params!(TimestampTz) => AggregateFunc::MinTimestampTz
            },
            "json_agg" => {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("json_agg"))
            },
            "jsonb_agg" => {
                params!(JsonbAny) => unary_op(|_ecx, e| {
                    // `AggregateFunc::JsonbAgg` filters out `Datum::Null` (it
                    // needs to have *some* identity input), but the semantics
                    // of the SQL function require that `Datum::Null` is treated
                    // as `Datum::JsonbNull`. This call to `coalesce` converts
                    // between the two semantics.
                    let json_null = ScalarExpr::literal(Datum::JsonNull, ScalarType::Jsonb);
                    let e = ScalarExpr::CallVariadic {
                        func: VariadicFunc::Coalesce,
                        exprs: vec![e, json_null],
                    };
                    Ok((e, AggregateFunc::JsonbAgg))
                })
            },
            "string_agg" => {
                params!(Any, String) => binary_op(|_ecx, _lhs, _rhs| unsupported!("string_agg"))
            },
            "sum" => {
                params!(Int32) => AggregateFunc::SumInt32,
                params!(Int64) => AggregateFunc::SumInt64,
                params!(Float32) => AggregateFunc::SumFloat32,
                params!(Float64) => AggregateFunc::SumFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::SumDecimal,
                params!(Interval) => unary_op(|_ecx, _e| {
                    // Explicitly providing this unsupported overload
                    // prevents `sum(NULL)` from choosing the `Float64`
                    // implementation, so that we match PostgreSQL's behavior.
                    // Plus we will one day want to support this overload.
                    unsupported!("sum(interval)");
                })
            }
        }
    };
}

pub fn is_aggregate_func(ident: &str) -> bool {
    BUILTIN_AGGREGATE_IMPLS.get(ident).is_some()
}

/// Plans a built-in aggregate function.
pub fn select_aggregate_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<(ScalarExpr, AggregateFunc), anyhow::Error> {
    let impls = match BUILTIN_AGGREGATE_IMPLS.get(ident) {
        Some(i) => i,
        None => bail!("function \"{}\" does not exist", ident),
    };

    let mut cexprs = Vec::new();
    for arg in args {
        let cexpr = query::plan_expr(ecx, arg)?;
        cexprs.push(cexpr);
    }

    ArgImplementationMatcher::select_implementation(ident, func_err_string, ecx, impls, cexprs)
}
