// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use semver::Version;
use std::collections::BTreeMap;
use tracing::info;

use mz_ore::collections::CollectionExt;
use mz_sql::ast::{
    display::AstDisplay, CreateSourceStatement, CreateSourceSubsource, CreateSubsourceOption,
    CreateSubsourceOptionName, DeferredObjectName, Raw, RawObjectName, ReferencedSubsources,
    Statement, UnresolvedObjectName, Value, WithOptionValue,
};

use crate::catalog::{Catalog, ConnCatalog, SerializedCatalogItem, SYSTEM_CONN_ID};

use super::storage::Transaction;

fn rewrite_items<F>(tx: &mut Transaction, mut f: F) -> Result<(), anyhow::Error>
where
    F: FnMut(&mut Transaction, &mut mz_sql::ast::Statement<Raw>) -> Result<(), anyhow::Error>,
{
    let mut updated_items = BTreeMap::new();
    let items = tx.loaded_items();
    for (id, name, SerializedCatalogItem::V1 { create_sql }) in items {
        let mut stmt = mz_sql::parse::parse(&create_sql)?.into_element();

        f(tx, &mut stmt)?;

        let serialized_item = SerializedCatalogItem::V1 {
            create_sql: stmt.to_ast_string_stable(),
        };

        updated_items.insert(id, (name.item, serialized_item));
    }
    tx.update_items(updated_items)?;
    Ok(())
}

pub(crate) async fn migrate(catalog: &mut Catalog) -> Result<(), anyhow::Error> {
    let mut storage = catalog.storage().await;
    let catalog_version = storage.get_catalog_content_version().await?;
    let catalog_version = match catalog_version {
        Some(v) => Version::parse(&v)?,
        None => Version::new(0, 0, 0),
    };

    info!("migrating from catalog version {:?}", catalog_version);

    let mut tx = storage.transaction().await?;
    // First, do basic AST -> AST transformations.
    rewrite_items(&mut tx, |_tx, _stmt| Ok(()))?;

    // Then, load up a temporary catalog with the rewritten items, and perform
    // some transformations that require introspecting the catalog. These
    // migrations are *weird*: they're rewriting the catalog while looking at
    // it. You probably should be adding a basic AST migration above, unless
    // you are really certain you want one of these crazy migrations.
    let cat = Catalog::load_catalog_items(&mut tx, catalog)?;
    let conn_cat = cat.for_system_session();
    rewrite_items(&mut tx, |tx, item| {
        deferred_object_name_rewrite(&conn_cat, item)?;
        progress_collection_rewrite(&conn_cat, tx, item)?;
        Ok(())
    })?;
    tx.commit().await?;
    info!(
        "migration from catalog version {:?} complete",
        catalog_version
    );
    Ok(())
}

// Add new migrations below their appropriate heading, and precede them with a
// short summary of the migration's purpose and optional additional commentary
// about safety or approach.
//
// The convention is to name the migration function using snake case:
// > <category>_<description>_<version>
//
// Note that:
// - The sum of all migrations must be idempotent because all migrations run
//   every time the catalog opens, unless migrations are explicitly disabled.
//   This might mean changing code outside the migration itself, or only
//   executing some migrations when encountering certain versions.
// - Migrations must preserve backwards compatibility with all past releases of
//   Materialize.
//
// Please include @benesch on any code reviews that add or edit migrations.

// ****************************************************************************
// AST migrations -- Basic AST -> AST transformations
// ****************************************************************************

// ****************************************************************************
// Semantic migrations -- Weird migrations that require access to the catalog
// ****************************************************************************

// Rewrites all subsource references to be qualified by their IDs, which is the
// mechanism by which `DeferredObjectName` differentiates between user input and
// created objects.
fn deferred_object_name_rewrite(
    cat: &ConnCatalog,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    if let Statement::CreateSource(CreateSourceStatement {
        referenced_subsources: Some(ReferencedSubsources::Subset(create_source_subsources)),
        ..
    }) = stmt
    {
        for CreateSourceSubsource { subsource, .. } in create_source_subsources {
            let object_name = subsource.as_mut().unwrap();
            let name: UnresolvedObjectName = match object_name {
                DeferredObjectName::Deferred(name) => name.clone(),
                DeferredObjectName::Named(..) => continue,
            };

            let partial_subsource_name =
                mz_sql::normalize::unresolved_object_name(name.clone()).expect("resolvable");
            let qualified_subsource_name = cat
                .resolve_item_name(&partial_subsource_name)
                .expect("known to exist");
            let entry = cat
                .state
                .try_get_entry_in_schema(qualified_subsource_name, SYSTEM_CONN_ID)
                .expect("known to exist");
            let id = entry.id();

            *subsource = Some(DeferredObjectName::Named(RawObjectName::Id(
                id.to_string(),
                name,
            )));
        }
    }
    Ok(())
}

// Rewrites all subsource references to be qualified by their IDs, which is the
// mechanism by which `DeferredObjectName` differentiates between user input and
// created objects.
fn progress_collection_rewrite(
    cat: &ConnCatalog<'_>,
    tx: &mut Transaction<'_>,
    stmt: &mut mz_sql::ast::Statement<Raw>,
) -> Result<(), anyhow::Error> {
    use mz_sql::ast::CreateSourceConnection;

    if let Statement::CreateSource(CreateSourceStatement {
        name,
        connection,
        progress_subsource,
        ..
    }) = stmt
    {
        if progress_subsource.is_some() {
            return Ok(());
        }

        let progress_desc = match connection {
            CreateSourceConnection::Kafka(_) => {
                mz_storage_client::types::sources::KAFKA_PROGRESS_DESC.clone()
            }
            CreateSourceConnection::Kinesis { .. } => {
                mz_storage_client::types::sources::KINESIS_PROGRESS_DESC.clone()
            }
            CreateSourceConnection::S3 { .. } => {
                mz_storage_client::types::sources::S3_PROGRESS_DESC.clone()
            }
            CreateSourceConnection::Postgres { .. } => {
                mz_storage_client::types::sources::PG_PROGRESS_DESC.clone()
            }
            CreateSourceConnection::LoadGenerator { .. } => {
                mz_storage_client::types::sources::LOADGEN_PROGRESS_DESC.clone()
            }
            CreateSourceConnection::TestScript { .. } => {
                mz_storage_client::types::sources::TEST_SCRIPT_PROGRESS_DESC.clone()
            }
        };

        // Generate a new GlobalId for the subsource.
        let progress_subsource_id =
            mz_repr::GlobalId::User(tx.get_and_increment_id("user".to_string())?);

        // Generate a StatementContext, which is simplest for handling names.
        let scx = mz_sql::plan::StatementContext::new(None, cat);

        // Find an available name.
        let (item, prefix) = name.0.split_last().unwrap();
        let mut suggested_name = prefix.to_vec();
        suggested_name.push(format!("{}_progress", item.as_str()).into());

        let partial =
            mz_sql::normalize::unresolved_object_name(UnresolvedObjectName(suggested_name))?;
        let qualified = scx.allocate_qualified_name(partial)?;
        let found_name = scx.catalog.find_available_name(qualified);
        let full_name = scx.catalog.resolve_full_name(&found_name);

        // Grab the item name, which is necessary to add this item to the
        // current transaction.
        let item_name = full_name.item.clone();

        info!(
            "adding progress subsource to {:?}; named {:?}, with id {:?}",
            name, full_name, progress_subsource_id,
        );

        // Generate an unresolved version of the name, which will
        // ultimately go in the parent `CREATE SOURCE` statement.
        let name = UnresolvedObjectName::from(full_name);

        // Generate the progress subsource schema.
        let (columns, table_constraints) = scx.relation_desc_into_table_defs(&progress_desc)?;

        // Create the subsource statement
        let subsource = mz_sql::ast::CreateSubsourceStatement {
            name: name.clone(),
            columns,
            constraints: table_constraints,
            if_not_exists: false,
            with_options: vec![CreateSubsourceOption {
                name: CreateSubsourceOptionName::Progress,
                value: Some(WithOptionValue::Value(Value::Boolean(true))),
            }],
        };

        tx.insert_item(
            progress_subsource_id,
            found_name.qualifiers.schema_spec.into(),
            &item_name,
            SerializedCatalogItem::V1 {
                create_sql: subsource.to_ast_string_stable(),
            },
        )?;

        // Place the newly created subsource into the `CREATE SOURCE` statement.
        *progress_subsource = Some(DeferredObjectName::Named(RawObjectName::Id(
            progress_subsource_id.to_string(),
            name,
        )));
    }
    Ok(())
}
