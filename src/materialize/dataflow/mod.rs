// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Driver for timely/differential dataflow.

mod arrangement;
mod optimize;
mod render;
mod sink;
mod source;
mod types;

pub mod func;
pub mod server;
pub mod transform;

pub use server::{serve, DataflowResultsHandler};
pub use types::*;
