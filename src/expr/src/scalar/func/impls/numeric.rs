// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::adt::numeric::{self, Numeric};

sqlfunc!(
    #[sqlname = "-"]
    fn neg_numeric(a: Numeric) -> Numeric {
        let mut a = a;
        numeric::cx_datum().neg(&mut a);
        numeric::munge_numeric(&mut a).unwrap();
        a
    }
);

sqlfunc!(
    #[sqlname = "abs"]
    fn abs_numeric(a: Numeric) -> Numeric {
        let mut a = a;
        numeric::cx_datum().abs(&mut a);
        a
    }
);
