// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::trace::{Batch, Builder, Trace, TraceReader};
use differential_dataflow::Data;
use timely::dataflow::Scope;

use crate::extensions::arrange::ArrangementSize;

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<G: Scope, K: Data, V: Data, R: Semigroup>:
    ReduceCore<G, K, V, R>
where
    G::Timestamp: Lattice + Ord,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, T2>(&self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: Trace
            + for<'a> TraceReader<Key<'a> = &'a K, KeyOwned = K, Time = G::Timestamp>
            + 'static,
        T2::ValOwned: Data,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        T2::Builder:
            Builder<Output = T2::Batch, Item = ((T2::KeyOwned, T2::ValOwned), T2::Time, T2::Diff)>,
        L: FnMut(&K, &[(&V, R)], &mut Vec<(T2::ValOwned, T2::Diff)>) + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        // Allow access to `reduce_abelian` since we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.reduce_abelian::<_, T2>(name, logic)
            .log_arrangement_size()
    }
}

impl<G, K, V, T1, R> MzReduce<G, K, V, R> for Arranged<G, T1>
where
    G::Timestamp: Lattice + Ord,
    G: Scope,
    K: Data,
    V: Data,
    R: Semigroup,
    T1: for<'a> TraceReader<
            Key<'a> = &'a K,
            KeyOwned = K,
            Val<'a> = &'a V,
            Time = G::Timestamp,
            Diff = R,
        > + Clone
        + 'static,
{
}

/// Extension trait for `ReduceCore`, currently providing a reduction based
/// on an operator-pair approach.
pub trait ReduceExt<G: Scope, K: Data, V: Data, R: Semigroup>
where
    G::Timestamp: Lattice + Ord,
{
    /// This method produces a reduction pair based on the same input arrangement. Each reduction
    /// in the pair operates with its own logic and the two output arrangements from the reductions
    /// are produced as a result. The method is useful for reductions that need to present different
    /// output views on the same input data. An example is producing an error-free reduction output
    /// along with a separate error output indicating when the error-free output is valid.
    fn reduce_pair<L1, T1, L2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: Trace
            + for<'a> TraceReader<Key<'a> = &'a K, KeyOwned = K, Time = G::Timestamp>
            + 'static,
        T1::ValOwned: Data,
        T1::Diff: Abelian,
        T1::Batch: Batch,
        T1::Builder:
            Builder<Output = T1::Batch, Item = ((T1::KeyOwned, T1::ValOwned), T1::Time, T1::Diff)>,
        L1: FnMut(&K, &[(&V, R)], &mut Vec<(T1::ValOwned, T1::Diff)>) + 'static,
        T2: Trace
            + for<'a> TraceReader<Key<'a> = &'a K, KeyOwned = K, Time = G::Timestamp>
            + 'static,
        T2::ValOwned: Data,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        T2::Builder:
            Builder<Output = T2::Batch, Item = ((T2::KeyOwned, T2::ValOwned), T2::Time, T2::Diff)>,
        L2: FnMut(&K, &[(&V, R)], &mut Vec<(T2::ValOwned, T2::Diff)>) + 'static,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize;
}

impl<G: Scope, K: Data, V: Data, Tr, R: Semigroup> ReduceExt<G, K, V, R> for Arranged<G, Tr>
where
    G::Timestamp: Lattice + Ord,
    Tr: for<'a> TraceReader<
            Key<'a> = &'a K,
            KeyOwned = K,
            Val<'a> = &'a V,
            Time = G::Timestamp,
            Diff = R,
        > + Clone
        + 'static,
{
    fn reduce_pair<L1, T1, L2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: Trace
            + for<'a> TraceReader<Key<'a> = &'a K, KeyOwned = K, Time = G::Timestamp>
            + 'static,
        T1::ValOwned: Data,
        T1::Diff: Abelian,
        T1::Batch: Batch,
        T1::Builder:
            Builder<Output = T1::Batch, Item = ((T1::KeyOwned, T1::ValOwned), T1::Time, T1::Diff)>,
        L1: FnMut(&K, &[(&V, R)], &mut Vec<(T1::ValOwned, T1::Diff)>) + 'static,
        T2: Trace
            + for<'a> TraceReader<Key<'a> = &'a K, KeyOwned = K, Time = G::Timestamp>
            + 'static,
        T2::ValOwned: Data,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        T2::Builder:
            Builder<Output = T2::Batch, Item = ((T2::KeyOwned, T2::ValOwned), T2::Time, T2::Diff)>,
        L2: FnMut(&K, &[(&V, R)], &mut Vec<(T2::ValOwned, T2::Diff)>) + 'static,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        let arranged1 = self.mz_reduce_abelian::<L1, T1>(name1, logic1);
        let arranged2 = self.mz_reduce_abelian::<L2, T2>(name2, logic2);
        (arranged1, arranged2)
    }
}
