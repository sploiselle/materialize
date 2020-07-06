// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use sql::names::FullName;
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{self, Expr, Ident, ObjectName, Query, Statement};

/// Rewrites all references of `from_name` to `to_name` or fails if the request
/// is ambiguous.
///
/// Ambiguous rename requests use
pub fn rewrite_create_stmt(
    source: &mut Statement,
    from_name: &FullName,
    to_name: &FullName,
) -> Result<(), String> {
    let from_ident = ObjectName::from(from_name).to_string();
    let maybe_update_object_name = |object_name: &mut ObjectName| {
        // `ObjectName` sensibly doesn't `impl PartialEq`, so we have to cheat
        // it.
        if object_name.to_string() == from_ident {
            object_name.0[2] = Ident::new(to_name.item.clone());
        }
    };

    match source {
        Statement::CreateView { name, query, .. } => {
            maybe_update_object_name(name);
            rewrite_query(from_name, to_name, query)?;
            // Ensure that our rewrite didn't didn't introduce an ambiguity on
            // `to_name`.
            assess_query_ambiguity(to_name, query)?;
        }
        Statement::CreateSource { name, .. } => {
            maybe_update_object_name(name);
        }
        Statement::CreateSink { name, from, .. } => {
            maybe_update_object_name(name);
            maybe_update_object_name(from);
        }
        Statement::CreateIndex { name, on_name, .. } => {
            let idents = &on_name.0;
            // Determine if the database and schema match.
            let db_schema_match = idents[0].to_string() == from_name.database.to_string()
                && idents[1].to_string() == from_name.schema;

            // Maybe rename the index itself...
            if db_schema_match && name.as_ref().unwrap().to_string() == from_name.item {
                *name = Some(Ident::new(to_name.item.clone()));
            // ...or its parent item.
            } else if idents[2].to_string() == from_name.item {
                on_name.0[2] = Ident::new(to_name.item.clone());
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

/// Rewrites `query`'s references of `from` to `to` or errors if too ambiguous.
fn rewrite_query(from: &FullName, to: &FullName, query: &mut Query) -> Result<(), String> {
    let from_ident = Ident::new(from.item.clone());
    let qual_depth = QueryIdentAgg::determine_qual_depth(&from_ident, query)?;
    println!("\nDetermined qual_depth {:?}", qual_depth);
    CreateSqlRewriter::rewrite_query_with_qual_depth(from, to, qual_depth, query);
    Ok(())
}

/// Determine if `name` is used ambiguously in `query`.
fn assess_query_ambiguity(name: &FullName, query: &Query) -> Result<(), String> {
    let name = Ident::new(name.item.clone());
    println!("\nDetermining ambiguity\n");
    match QueryIdentAgg::determine_qual_depth(&name, query) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Visits a [`Query`], assessing catalog item [`Ident`]s' use of a specified `Ident`.
struct QueryIdentAgg<'a> {
    /// The name whose usage you want to assess.
    name: &'a Ident,
    /// Tracks all second-level qualifiers used on `name` in a `HashMap`, as
    /// well as any third-level qualifiers used on those second-level qualifiers
    /// in a `HashSet`.
    qualifiers: HashMap<Ident, HashSet<Ident>>,
    all_item_names: HashSet<Ident>,
    /// Tracks the least qualified instance of `name` seen.
    min_qual_depth: usize,
    err: Option<String>,
}

impl<'a> QueryIdentAgg<'a> {
    /// Determines the depth of qualification needed to unambiguously reference
    /// catalog items in a [`Query`].
    ///
    /// Includes an option to fail if a given `Ident` is encountered.
    ///
    /// `Result`s of `Ok(usize)` indicate that `name` can be unambiguously
    /// referred to with `usize` parts, e.g. 2 requires schema and item name
    /// qualification.
    ///
    /// `Result`s of `Err` indicate that we cannot unambiguously reference
    /// `name`, or if `fail_on` was provided, that it encountered the specified
    /// `Ident`.
    fn determine_qual_depth(name: &Ident, query: &Query) -> Result<usize, String> {
        let mut v = QueryIdentAgg {
            qualifiers: HashMap::new(),
            all_item_names: HashSet::new(),
            min_qual_depth: usize::MAX,
            err: None,
            name,
        };

        v.visit_query(query);

        if let Some(e) = v.err {
            return Err(e);
        }

        // We cannot disambiguate items where `name` is used to qualify itself.
        // e.g. if we encounter `a.b.a` we cannot determine which level of
        // qualification `a` applies to.
        // if v.qualifiers.values().any(|t| t.contains(&name)) ||  {
        //     return Err(format!("{} used to qualify item with same name", name));
        // }
        // Check if there was more than one 3rd-level (e.g.
        // database) qualification used for any reference to `name`.
        let req_depth = if v.qualifiers.values().any(|v| v.len() > 1)
            || v.qualifiers.values().any(|t| t.contains(&name))
        {
            3
        // Check if there was more than one 2nd-level (e.g. schema)
        // qualification used for any reference to `name`.
        } else if v.qualifiers.len() > 1 || v.qualifiers.contains_key(&name) {
            2
        } else {
            return Ok(1);
        };

        if v.min_qual_depth < req_depth {
            Err(format!("{} used ambiguously", name))
        } else {
            Ok(req_depth)
        }
    }

    fn aggregate_names(&mut self, v: &[Ident]) {
        println!("Looking at {:?}", v);
        if v.len() == 1 || self.err.is_some() {
            return;
        } else if v.len() == 3 {
            // Their name used as our qualifier
            if self.qualifiers.contains_key(&v[2]) {
                self.err = Some(format!(
                    "{} is ambiguously used as both item and column",
                    self.name
                ));
            }
            // Our name used as their qualifier
            if &v[1] == self.name {
                self.err = Some(format!(
                    "{} is ambiguously used as both schema and item",
                    self.name
                ));
            }
            // Our name used as database (we know it's not a schema, because
            // that will result in an error).
            if &v[0] == self.name {
                self.qualifiers
                    .entry(v[1].clone())
                    .or_default()
                    .insert(v[0].clone());
            }
            self.all_item_names.insert(v[2].clone());
            println!(
                "Adding {:?} to all_item_names:\n{:?}",
                v[2], self.all_item_names
            );
        }

        if let Some(p) = v.iter().rposition(|i| i == self.name) {
            let i = v[..p + 1].to_vec();
            let mut i_len = i.len();
            println!("Found match {:?}", i);
            match i_len {
                1 => if v.len() == 3 {},
                2 => {
                    if &i[0] == self.name {
                        i_len = 1;
                    }
                }
                3 => {
                    println!(
                        "Checking against_all_item_names:\n{:?}",
                        self.all_item_names
                    );
                    // Our qualifier is their item name.
                    if self.all_item_names.contains(&i[1]) {
                        if self.all_item_names.contains(&i[1]) {
                            println!("\n\n\n\nMY SCHEMA NAME USED AS AN ITEM NAME\n\n\n\n");
                        }
                        println!(
                            "ERROR: {} ambiguously used as either item or column",
                            self.name
                        );
                        self.err = Some(format!(
                            "{} ambiguously used as either item or column",
                            self.name
                        ));
                    }

                    // 3rd-level qualification on `self.name`.
                    self.qualifiers
                        .entry(i[1].clone())
                        .or_default()
                        .insert(i[0].clone());
                }
                4 => {
                    if &i[2] == self.name {
                        i_len = 3;
                        self.qualifiers
                            .entry(i[1].clone())
                            .or_default()
                            .insert(i[0].clone());
                    }
                }
                _ => unreachable!(),
            }
            self.min_qual_depth = std::cmp::min(i_len, self.min_qual_depth);
        }
    }
}

impl<'a, 'ast> Visit<'ast> for QueryIdentAgg<'a> {
    fn visit_query(&mut self, query: &'ast Query) {
        visit::visit_query(self, query);
    }
    fn visit_expr(&mut self, e: &'ast Expr) {
        match e {
            Expr::Identifier(i) | Expr::QualifiedWildcard(i) => {
                self.aggregate_names(i);
            }
            _ => visit::visit_expr(self, e),
        }
    }
    fn visit_ident(&mut self, ident: &'ast Ident) {
        // This is an unqualified item using `self.name`, e.g. an alias or a
        // column, neither of which we can unambiguously resolve.
        if ident == self.name {
            self.err = Some(format!("{} used as either column or alias name", self.name));
        }
    }
    fn visit_object_name(&mut self, object_name: &'ast ObjectName) {
        self.aggregate_names(&object_name.0);
    }
    fn visit_table_alias(&mut self, alias: &'ast ast::TableAlias) {
        if &alias.name == self.name {
            self.err = Some(format!(
                "{} used as an alias, which introduces ambiguities",
                self.name
            ));
        }
    }
}

struct CreateSqlRewriter {
    from: Vec<Ident>,
    to: Vec<Ident>,
}

impl CreateSqlRewriter {
    fn rewrite_query_with_qual_depth(
        from_name: &FullName,
        to_name: &FullName,
        qual_depth: usize,
        query: &mut Query,
    ) {
        let (from, to) = match qual_depth {
            1 => (
                vec![Ident::new(from_name.item.clone())],
                vec![Ident::new(to_name.item.clone())],
            ),
            2 => (
                vec![
                    Ident::new(from_name.schema.clone()),
                    Ident::new(from_name.item.clone()),
                ],
                vec![
                    Ident::new(to_name.schema.clone()),
                    Ident::new(to_name.item.clone()),
                ],
            ),
            3 => (
                vec![
                    Ident::new(from_name.database.to_string()),
                    Ident::new(from_name.schema.clone()),
                    Ident::new(from_name.item.clone()),
                ],
                vec![
                    Ident::new(to_name.database.to_string()),
                    Ident::new(to_name.schema.clone()),
                    Ident::new(to_name.item.clone()),
                ],
            ),
            _ => unreachable!(),
        };
        let mut v = CreateSqlRewriter { from, to };
        v.visit_query_mut(query);
    }

    fn maybe_rewrite_idents(&mut self, h: &mut Vec<Ident>) {
        // We don't want to rewrite if the item we're rewriting is shorter than
        // the values we want to replace them with. This should never happen,
        // but panics if it does, so rather safe than sorry.
        macro_rules! rewrite_to {
            ($i: expr) => {
                if self.from.len() > $i {
                    return;
                }
                let n = &self.from;

                for i in 0..$i - n.len() + 1 {
                    // If subset of `h` matches `self.from`...
                    if h[i..i + n.len()] == n[..] {
                        // ...splice `self.to` into `h` in that subset's location.
                        h.splice(i..i + n.len(), self.to.iter().cloned());
                        return;
                    }
                }
            };
        }
        match h.len() {
            1 => {}
            2 => {
                rewrite_to!(1);
            }
            3 | 4 => {
                rewrite_to!(3);
            }
            _ => unreachable!(),
        }
    }
}

impl<'ast> VisitMut<'ast> for CreateSqlRewriter {
    fn visit_query_mut(&mut self, query: &'ast mut Query) {
        visit_mut::visit_query_mut(self, query);
    }
    fn visit_expr_mut(&mut self, e: &'ast mut Expr) {
        match e {
            Expr::Identifier(ref mut i) | Expr::QualifiedWildcard(ref mut i) => {
                self.maybe_rewrite_idents(i);
            }
            _ => visit_mut::visit_expr_mut(self, e),
        }
    }
    fn visit_object_name_mut(&mut self, object_name: &'ast mut ObjectName) {
        self.maybe_rewrite_idents(&mut object_name.0);
    }
}
