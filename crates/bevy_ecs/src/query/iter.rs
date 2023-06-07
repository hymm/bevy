use crate::{
    archetype::{Archetype, ArchetypeEntity, ArchetypeId, Archetypes},
    component::Tick,
    entity::{Entities, Entity},
    query::{ArchetypeFilter, DebugCheckedUnwrap, QueryState},
    storage::{Table, TableId, TableRow, Tables},
    world::unsafe_world_cell::UnsafeWorldCell,
};
use std::{borrow::Borrow, iter::FusedIterator, mem::MaybeUninit, ops::Range};

use super::{QueryData, QueryFilter, ReadOnlyQueryData};

/// An [`Iterator`] over query results of a [`Query`](crate::system::Query).
///
/// This struct is created by the [`Query::iter`](crate::system::Query::iter) and
/// [`Query::iter_mut`](crate::system::Query::iter_mut) methods.
pub struct QueryIter<'w, 's, D: QueryData, F: QueryFilter> {
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    query_state: &'s QueryState<D, F>,
    cursor: QueryIterationCursor<'w, 's, D, F>,
}

impl<'w, 's, D: QueryData, F: QueryFilter> QueryIter<'w, 's, D, F> {
    /// # Safety
    /// - `world` must have permission to access any of the components registered in `query_state`.
    /// - `world` must be the same one used to initialize `query_state`.
    pub(crate) unsafe fn new(
        world: UnsafeWorldCell<'w>,
        query_state: &'s QueryState<D, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        QueryIter {
            query_state,
            // SAFETY: We only access table data that has been registered in `query_state`.
            tables: &world.storages().tables,
            archetypes: world.archetypes(),
            cursor: QueryIterationCursor::init(world, query_state, last_run, this_run),
        }
    }

    /// Executes the equivalent of [`Iterator::for_each`] over a contiguous segment
    /// from an table.
    ///
    /// # Safety
    ///  - all `rows` must be in `[0, table.entity_count)`.
    ///  - `table` must match D and F
    ///  - Both `D::IS_DENSE` and `F::IS_DENSE` must be true.
    #[inline]
    #[cfg(all(not(target = "wasm32"), feature = "multi-threaded"))]
    pub(super) unsafe fn for_each_in_table_range<Func>(
        &mut self,
        func: &mut Func,
        table: &'w Table,
        rows: Range<usize>,
    ) where
        Func: FnMut(D::Item<'w>),
    {
        // SAFETY: Caller assures that D::IS_DENSE and F::IS_DENSE are true, that table matches D and F
        // and all indicies in rows are in range.
        unsafe {
            self.fold_over_table_range((), &mut |_, item| func(item), table, rows);
        }
    }

    /// Executes the equivalent of [`Iterator::for_each`] over a contiguous segment
    /// from an archetype.
    ///
    /// # Safety
    ///  - all `indices` must be in `[0, archetype.len())`.
    ///  - `archetype` must match D and F
    ///  - Either `D::IS_DENSE` or `F::IS_DENSE` must be false.
    #[inline]
    #[cfg(all(not(target = "wasm32"), feature = "multi-threaded"))]
    pub(super) unsafe fn for_each_in_archetype_range<Func>(
        &mut self,
        func: &mut Func,
        archetype: &'w Archetype,
        rows: Range<usize>,
    ) where
        Func: FnMut(D::Item<'w>),
    {
        // SAFETY: Caller assures that either D::IS_DENSE or F::IS_DENSE are false, that archetype matches D and F
        // and all indices in rows are in range.
        unsafe {
            self.fold_over_archetype_range((), &mut |_, item| func(item), archetype, rows);
        }
    }

    /// Executes the equivalent of [`Iterator::fold`] over a contiguous segment
    /// from an table.
    ///
    /// # Safety
    ///  - all `rows` must be in `[0, table.entity_count)`.
    ///  - `table` must match D and F
    ///  - Both `D::IS_DENSE` and `F::IS_DENSE` must be true.
    #[inline]
    pub(super) unsafe fn fold_over_table_range<B, Func>(
        &mut self,
        mut accum: B,
        func: &mut Func,
        table: &'w Table,
        rows: Range<usize>,
    ) -> B
    where
        Func: FnMut(B, D::Item<'w>) -> B,
    {
        assert!(
            rows.end <= u32::MAX as usize,
            "TableRow is only valid up to u32::MAX"
        );

        D::set_table(&mut self.cursor.fetch, &self.query_state.fetch_state, table);
        F::set_table(
            &mut self.cursor.filter,
            &self.query_state.filter_state,
            table,
        );

        let entities = table.entities();
        for row in rows {
            // SAFETY: Caller assures `row` in range of the current archetype.
            let entity = entities.get_unchecked(row);
            let row = TableRow::from_usize(row);
            // SAFETY: set_table was called prior.
            // Caller assures `row` in range of the current archetype.
            if !F::filter_fetch(&mut self.cursor.filter, *entity, row) {
                continue;
            }

            // SAFETY: set_table was called prior.
            // Caller assures `row` in range of the current archetype.
            let item = D::fetch(&mut self.cursor.fetch, *entity, row);

            accum = func(accum, item);
        }
        accum
    }

    /// Executes the equivalent of [`Iterator::fold`] over a contiguous segment
    /// from an archetype.
    ///
    /// # Safety
    ///  - all `indices` must be in `[0, archetype.len())`.
    ///  - `archetype` must match D and F
    ///  - Either `D::IS_DENSE` or `F::IS_DENSE` must be false.
    #[inline]
    pub(super) unsafe fn fold_over_archetype_range<B, Func>(
        &mut self,
        mut accum: B,
        func: &mut Func,
        archetype: &'w Archetype,
        indices: Range<usize>,
    ) -> B
    where
        Func: FnMut(B, D::Item<'w>) -> B,
    {
        let table = self.tables.get(archetype.table_id()).debug_checked_unwrap();
        D::set_archetype(
            &mut self.cursor.fetch,
            &self.query_state.fetch_state,
            archetype,
            table,
        );
        F::set_archetype(
            &mut self.cursor.filter,
            &self.query_state.filter_state,
            archetype,
            table,
        );

        let entities = archetype.entities();
        for index in indices {
            // SAFETY: Caller assures `index` in range of the current archetype.
            let archetype_entity = entities.get_unchecked(index);
            // SAFETY: set_archetype was called prior.
            // Caller assures `index` in range of the current archetype.
            if !F::filter_fetch(
                &mut self.cursor.filter,
                archetype_entity.id(),
                archetype_entity.table_row(),
            ) {
                continue;
            }

            // SAFETY: set_archetype was called prior, `index` is an archetype index in range of the current archetype
            // Caller assures `index` in range of the current archetype.
            let item = D::fetch(
                &mut self.cursor.fetch,
                archetype_entity.id(),
                archetype_entity.table_row(),
            );

            accum = func(accum, item);
        }
        accum
    }
}

impl<'w, 's, D: QueryData, F: QueryFilter> Iterator for QueryIter<'w, 's, D, F> {
    type Item = D::Item<'w>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY:
        // `tables` and `archetypes` belong to the same world that the cursor was initialized for.
        // `query_state` is the state that was passed to `QueryIterationCursor::init`.
        unsafe {
            self.cursor
                .next(self.tables, self.archetypes, self.query_state)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let max_size = self.cursor.max_remaining(self.tables, self.archetypes);
        let archetype_query = F::IS_ARCHETYPAL;
        let min_size = if archetype_query { max_size } else { 0 };
        (min_size, Some(max_size))
    }

    #[inline]
    fn fold<B, Func>(mut self, init: B, mut func: Func) -> B
    where
        Func: FnMut(B, Self::Item) -> B,
    {
        let mut accum = init;
        // Empty any remaining uniterated values from the current table/archetype
        while self.cursor.current_row != self.cursor.current_len {
            let Some(item) = self.next() else { break };
            accum = func(accum, item);
        }
        if D::IS_DENSE && F::IS_DENSE {
            for table_id in self.cursor.table_id_iter.clone() {
                // SAFETY: Matched table IDs are guaranteed to still exist.
                let table = unsafe { self.tables.get(*table_id).debug_checked_unwrap() };
                accum =
                    // SAFETY: 
                    // - The fetched table matches both D and F
                    // - The provided range is equivalent to [0, table.entity_count)
                    // - The if block ensures that D::IS_DENSE and F::IS_DENSE are both true
                    unsafe { self.fold_over_table_range(accum, &mut func, table, 0..table.entity_count()) };
            }
        } else {
            for archetype_id in self.cursor.archetype_id_iter.clone() {
                let archetype =
                    // SAFETY: Matched archetype IDs are guaranteed to still exist.
                    unsafe { self.archetypes.get(*archetype_id).debug_checked_unwrap() };
                accum =
                    // SAFETY:
                    // - The fetched archetype matches both D and F
                    // - The provided range is equivalent to [0, archetype.len)
                    // - The if block ensures that ether D::IS_DENSE or F::IS_DENSE are false
                    unsafe { self.fold_over_archetype_range(accum, &mut func, archetype, 0..archetype.len()) };
            }
        }
        accum
    }
}

// This is correct as [`QueryIter`] always returns `None` once exhausted.
impl<'w, 's, D: QueryData, F: QueryFilter> FusedIterator for QueryIter<'w, 's, D, F> {}

/// An [`Iterator`] over the query items generated from an iterator of [`Entity`]s.
///
/// Items are returned in the order of the provided iterator.
/// Entities that don't match the query are skipped.
///
/// This struct is created by the [`Query::iter_many`](crate::system::Query::iter_many) and [`Query::iter_many_mut`](crate::system::Query::iter_many_mut) methods.
pub struct QueryManyIter<'w, 's, D: QueryData, F: QueryFilter, I: Iterator>
where
    I::Item: Borrow<Entity>,
{
    entity_iter: I,
    entities: &'w Entities,
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    fetch: D::Fetch<'w>,
    filter: F::Fetch<'w>,
    query_state: &'s QueryState<D, F>,
}

impl<'w, 's, D: QueryData, F: QueryFilter, I: Iterator> QueryManyIter<'w, 's, D, F, I>
where
    I::Item: Borrow<Entity>,
{
    /// # Safety
    /// - `world` must have permission to access any of the components registered in `query_state`.
    /// - `world` must be the same one used to initialize `query_state`.
    pub(crate) unsafe fn new<EntityList: IntoIterator<IntoIter = I>>(
        world: UnsafeWorldCell<'w>,
        query_state: &'s QueryState<D, F>,
        entity_list: EntityList,
        last_run: Tick,
        this_run: Tick,
    ) -> QueryManyIter<'w, 's, D, F, I> {
        let fetch = D::init_fetch(world, &query_state.fetch_state, last_run, this_run);
        let filter = F::init_fetch(world, &query_state.filter_state, last_run, this_run);
        QueryManyIter {
            query_state,
            entities: world.entities(),
            archetypes: world.archetypes(),
            // SAFETY: We only access table data that has been registered in `query_state`.
            // This means `world` has permission to access the data we use.
            tables: &world.storages().tables,
            fetch,
            filter,
            entity_iter: entity_list.into_iter(),
        }
    }

    /// Safety:
    /// The lifetime here is not restrictive enough for Fetch with &mut access,
    /// as calling `fetch_next_aliased_unchecked` multiple times can produce multiple
    /// references to the same component, leading to unique reference aliasing.
    ///
    /// It is always safe for shared access.
    #[inline(always)]
    unsafe fn fetch_next_aliased_unchecked(&mut self) -> Option<D::Item<'w>> {
        for entity in self.entity_iter.by_ref() {
            let entity = *entity.borrow();
            let Some(location) = self.entities.get(entity) else {
                continue;
            };

            if !self
                .query_state
                .matched_archetypes
                .contains(location.archetype_id.index())
            {
                continue;
            }

            let archetype = self
                .archetypes
                .get(location.archetype_id)
                .debug_checked_unwrap();
            let table = self.tables.get(location.table_id).debug_checked_unwrap();

            // SAFETY: `archetype` is from the world that `fetch/filter` were created for,
            // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
            D::set_archetype(
                &mut self.fetch,
                &self.query_state.fetch_state,
                archetype,
                table,
            );
            // SAFETY: `table` is from the world that `fetch/filter` were created for,
            // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
            F::set_archetype(
                &mut self.filter,
                &self.query_state.filter_state,
                archetype,
                table,
            );

            // SAFETY: set_archetype was called prior.
            // `location.archetype_row` is an archetype index row in range of the current archetype, because if it was not, the match above would have `continue`d
            if F::filter_fetch(&mut self.filter, entity, location.table_row) {
                // SAFETY:
                // - set_archetype was called prior, `location.archetype_row` is an archetype index in range of the current archetype
                // - fetch is only called once for each entity.
                return Some(D::fetch(&mut self.fetch, entity, location.table_row));
            }
        }
        None
    }

    /// Get next result from the query
    #[inline(always)]
    pub fn fetch_next(&mut self) -> Option<D::Item<'_>> {
        // SAFETY: we are limiting the returned reference to self,
        // making sure this method cannot be called multiple times without getting rid
        // of any previously returned unique references first, thus preventing aliasing.
        unsafe { self.fetch_next_aliased_unchecked().map(D::shrink) }
    }
}

impl<'w, 's, D: ReadOnlyQueryData, F: QueryFilter, I: Iterator> Iterator
    for QueryManyIter<'w, 's, D, F, I>
where
    I::Item: Borrow<Entity>,
{
    type Item = D::Item<'w>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: It is safe to alias for ReadOnlyWorldQuery.
        unsafe { self.fetch_next_aliased_unchecked() }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, max_size) = self.entity_iter.size_hint();
        (0, max_size)
    }
}

// This is correct as [`QueryManyIter`] always returns `None` once exhausted.
impl<'w, 's, D: ReadOnlyQueryData, F: QueryFilter, I: Iterator> FusedIterator
    for QueryManyIter<'w, 's, D, F, I>
where
    I::Item: Borrow<Entity>,
{
}

/// An iterator over `K`-sized combinations of query items without repetition.
///
/// A combination is an arrangement of a collection of items where order does not matter.
///
/// `K` is the number of items that make up each subset, and the number of items returned by the iterator.
/// `N` is the number of total entities output by the query.
///
/// For example, given the list [1, 2, 3, 4], where `K` is 2, the combinations without repeats are
/// [1, 2], [1, 3], [1, 4], [2, 3], [2, 4], [3, 4].
/// And in this case, `N` would be defined as 4 since the size of the input list is 4.
///
/// The number of combinations depend on how `K` relates to the number of entities matching the [`Query`]:
/// - if `K = N`, only one combination exists,
/// - if `K < N`, there are <sub>N</sub>C<sub>K</sub> combinations (see the [performance section] of `Query`),
/// - if `K > N`, there are no combinations.
///
/// The output combination is not guaranteed to have any order of iteration.
///
/// # Usage
///
/// This type is returned by calling [`Query::iter_combinations`] or [`Query::iter_combinations_mut`].
///
/// It implements [`Iterator`] only if it iterates over read-only query items ([learn more]).
///
/// In the case of mutable query items, it can be iterated by calling [`fetch_next`] in a `while let` loop.
///
/// # Examples
///
/// The following example shows how to traverse the iterator when the query items are read-only.
///
/// ```
/// # use bevy_ecs::prelude::*;
/// # #[derive(Component)]
/// # struct ComponentA;
/// #
/// fn some_system(query: Query<&ComponentA>) {
///     for [a1, a2] in query.iter_combinations() {
///         // ...
///     }
/// }
/// ```
///
/// The following example shows how `fetch_next` should be called with a `while let` loop to traverse the iterator when the query items are mutable.
///
/// ```
/// # use bevy_ecs::prelude::*;
/// # #[derive(Component)]
/// # struct ComponentA;
/// #
/// fn some_system(mut query: Query<&mut ComponentA>) {
///     let mut combinations = query.iter_combinations_mut();
///     while let Some([a1, a2]) = combinations.fetch_next() {
///         // ...
///     }
/// }
/// ```
///
/// [`fetch_next`]: Self::fetch_next
/// [learn more]: Self#impl-Iterator
/// [performance section]: crate::system::Query#performance
/// [`Query`]: crate::system::Query
/// [`Query::iter_combinations`]: crate::system::Query::iter_combinations
/// [`Query::iter_combinations_mut`]: crate::system::Query::iter_combinations_mut
pub struct QueryCombinationIter<'w, 's, D: QueryData, F: QueryFilter, const K: usize> {
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    query_state: &'s QueryState<D, F>,
    cursors: [QueryIterationCursor<'w, 's, D, F>; K],
}

impl<'w, 's, D: QueryData, F: QueryFilter, const K: usize> QueryCombinationIter<'w, 's, D, F, K> {
    /// # Safety
    /// - `world` must have permission to access any of the components registered in `query_state`.
    /// - `world` must be the same one used to initialize `query_state`.
    pub(crate) unsafe fn new(
        world: UnsafeWorldCell<'w>,
        query_state: &'s QueryState<D, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        // Initialize array with cursors.
        // There is no FromIterator on arrays, so instead initialize it manually with MaybeUninit

        let mut array: MaybeUninit<[QueryIterationCursor<'w, 's, D, F>; K]> = MaybeUninit::uninit();
        let ptr = array
            .as_mut_ptr()
            .cast::<QueryIterationCursor<'w, 's, D, F>>();
        if K != 0 {
            ptr.write(QueryIterationCursor::init(
                world,
                query_state,
                last_run,
                this_run,
            ));
        }
        for slot in (1..K).map(|offset| ptr.add(offset)) {
            slot.write(QueryIterationCursor::init_empty(
                world,
                query_state,
                last_run,
                this_run,
            ));
        }

        QueryCombinationIter {
            query_state,
            // SAFETY: We only access table data that has been registered in `query_state`.
            tables: &world.storages().tables,
            archetypes: world.archetypes(),
            cursors: array.assume_init(),
        }
    }

    /// Safety:
    /// The lifetime here is not restrictive enough for Fetch with &mut access,
    /// as calling `fetch_next_aliased_unchecked` multiple times can produce multiple
    /// references to the same component, leading to unique reference aliasing.
    ///.
    /// It is always safe for shared access.
    unsafe fn fetch_next_aliased_unchecked(&mut self) -> Option<[D::Item<'w>; K]> {
        if K == 0 {
            return None;
        }

        // PERF: can speed up the following code using `cursor.remaining()` instead of `next_item.is_none()`
        // when D::IS_ARCHETYPAL && F::IS_ARCHETYPAL
        //
        // let `i` be the index of `c`, the last cursor in `self.cursors` that
        // returns `K-i` or more elements.
        // Make cursor in index `j` for all `j` in `[i, K)` a copy of `c` advanced `j-i+1` times.
        // If no such `c` exists, return `None`
        'outer: for i in (0..K).rev() {
            match self.cursors[i].next(self.tables, self.archetypes, self.query_state) {
                Some(_) => {
                    for j in (i + 1)..K {
                        self.cursors[j] = self.cursors[j - 1].clone();
                        match self.cursors[j].next(self.tables, self.archetypes, self.query_state) {
                            Some(_) => {}
                            None if i > 0 => continue 'outer,
                            None => return None,
                        }
                    }
                    break;
                }
                None if i > 0 => continue,
                None => return None,
            }
        }

        let mut values = MaybeUninit::<[D::Item<'w>; K]>::uninit();

        let ptr = values.as_mut_ptr().cast::<D::Item<'w>>();
        for (offset, cursor) in self.cursors.iter_mut().enumerate() {
            ptr.add(offset).write(cursor.peek_last().unwrap());
        }

        Some(values.assume_init())
    }

    /// Get next combination of queried components
    #[inline]
    pub fn fetch_next(&mut self) -> Option<[D::Item<'_>; K]> {
        // SAFETY: we are limiting the returned reference to self,
        // making sure this method cannot be called multiple times without getting rid
        // of any previously returned unique references first, thus preventing aliasing.
        unsafe {
            self.fetch_next_aliased_unchecked()
                .map(|array| array.map(D::shrink))
        }
    }
}

// Iterator type is intentionally implemented only for read-only access.
// Doing so for mutable references would be unsound, because calling `next`
// multiple times would allow multiple owned references to the same data to exist.
impl<'w, 's, D: ReadOnlyQueryData, F: QueryFilter, const K: usize> Iterator
    for QueryCombinationIter<'w, 's, D, F, K>
{
    type Item = [D::Item<'w>; K];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Safety: it is safe to alias for ReadOnlyWorldQuery
        unsafe { QueryCombinationIter::fetch_next_aliased_unchecked(self) }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // binomial coefficient: (n ; k) = n! / k!(n-k)! = (n*n-1*...*n-k+1) / k!
        // See https://en.wikipedia.org/wiki/Binomial_coefficient
        // See https://blog.plover.com/math/choose.html for implementation
        // It was chosen to reduce overflow potential.
        fn choose(n: usize, k: usize) -> Option<usize> {
            if k > n || n == 0 {
                return Some(0);
            }
            let k = k.min(n - k);
            let ks = 1..=k;
            let ns = (n - k + 1..=n).rev();
            ks.zip(ns)
                .try_fold(1_usize, |acc, (k, n)| Some(acc.checked_mul(n)? / k))
        }
        // sum_i=0..k choose(cursors[i].remaining, k-i)
        let max_combinations = self
            .cursors
            .iter()
            .enumerate()
            .try_fold(0, |acc, (i, cursor)| {
                let n = cursor.max_remaining(self.tables, self.archetypes);
                Some(acc + choose(n, K - i)?)
            });

        let archetype_query = F::IS_ARCHETYPAL;
        let known_max = max_combinations.unwrap_or(usize::MAX);
        let min_combinations = if archetype_query { known_max } else { 0 };
        (min_combinations, max_combinations)
    }
}

impl<'w, 's, D: QueryData, F: QueryFilter> ExactSizeIterator for QueryIter<'w, 's, D, F>
where
    F: ArchetypeFilter,
{
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

// This is correct as [`QueryCombinationIter`] always returns `None` once exhausted.
impl<'w, 's, D: ReadOnlyQueryData, F: QueryFilter, const K: usize> FusedIterator
    for QueryCombinationIter<'w, 's, D, F, K>
{
}

struct QueryIterationCursor<'w, 's, D: QueryData, F: QueryFilter> {
    table_id_iter: std::slice::Iter<'s, TableId>,
    archetype_id_iter: std::slice::Iter<'s, ArchetypeId>,
    table_entities: &'w [Entity],
    archetype_entities: &'w [ArchetypeEntity],
    fetch: D::Fetch<'w>,
    filter: F::Fetch<'w>,
    // length of the table table or length of the archetype, depending on whether both `D`'s and `F`'s fetches are dense
    current_len: usize,
    // either table row or archetype index, depending on whether both `D`'s and `F`'s fetches are dense
    current_row: usize,
}

impl<D: QueryData, F: QueryFilter> Clone for QueryIterationCursor<'_, '_, D, F> {
    fn clone(&self) -> Self {
        Self {
            table_id_iter: self.table_id_iter.clone(),
            archetype_id_iter: self.archetype_id_iter.clone(),
            table_entities: self.table_entities,
            archetype_entities: self.archetype_entities,
            fetch: self.fetch.clone(),
            filter: self.filter.clone(),
            current_len: self.current_len,
            current_row: self.current_row,
        }
    }
}

impl<'w, 's, D: QueryData, F: QueryFilter> QueryIterationCursor<'w, 's, D, F> {
    const IS_DENSE: bool = D::IS_DENSE && F::IS_DENSE;

    unsafe fn init_empty(
        world: UnsafeWorldCell<'w>,
        query_state: &'s QueryState<D, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        QueryIterationCursor {
            table_id_iter: [].iter(),
            archetype_id_iter: [].iter(),
            ..Self::init(world, query_state, last_run, this_run)
        }
    }

    /// # Safety
    /// - `world` must have permission to access any of the components registered in `query_state`.
    /// - `world` must be the same one used to initialize `query_state`.
    unsafe fn init(
        world: UnsafeWorldCell<'w>,
        query_state: &'s QueryState<D, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        let fetch = D::init_fetch(world, &query_state.fetch_state, last_run, this_run);
        let filter = F::init_fetch(world, &query_state.filter_state, last_run, this_run);
        QueryIterationCursor {
            fetch,
            filter,
            table_entities: &[],
            archetype_entities: &[],
            table_id_iter: query_state.matched_table_ids.iter(),
            archetype_id_iter: query_state.matched_archetype_ids.iter(),
            current_len: 0,
            current_row: 0,
        }
    }

    /// retrieve item returned from most recent `next` call again.
    #[inline]
    unsafe fn peek_last(&mut self) -> Option<D::Item<'w>> {
        if self.current_row > 0 {
            let index = self.current_row - 1;
            if Self::IS_DENSE {
                let entity = self.table_entities.get_unchecked(index);
                Some(D::fetch(
                    &mut self.fetch,
                    *entity,
                    TableRow::from_usize(index),
                ))
            } else {
                let archetype_entity = self.archetype_entities.get_unchecked(index);
                Some(D::fetch(
                    &mut self.fetch,
                    archetype_entity.id(),
                    archetype_entity.table_row(),
                ))
            }
        } else {
            None
        }
    }

    /// How many values will this cursor return at most?
    ///
    /// Note that if `D::IS_ARCHETYPAL && F::IS_ARCHETYPAL`, the return value
    /// will be **the exact count of remaining values**.
    fn max_remaining(&self, tables: &'w Tables, archetypes: &'w Archetypes) -> usize {
        let remaining_matched: usize = if Self::IS_DENSE {
            let ids = self.table_id_iter.clone();
            ids.map(|id| tables[*id].entity_count()).sum()
        } else {
            let ids = self.archetype_id_iter.clone();
            ids.map(|id| archetypes[*id].len()).sum()
        };
        remaining_matched + self.current_len - self.current_row
    }

    // NOTE: If you are changing query iteration code, remember to update the following places, where relevant:
    // QueryIter, QueryIterationCursor, QueryManyIter, QueryCombinationIter, QueryState::par_for_each_unchecked_manual
    /// # Safety
    /// `tables` and `archetypes` must belong to the same world that the [`QueryIterationCursor`]
    /// was initialized for.
    /// `query_state` must be the same [`QueryState`] that was passed to `init` or `init_empty`.
    #[inline(always)]
    unsafe fn next(
        &mut self,
        tables: &'w Tables,
        archetypes: &'w Archetypes,
        query_state: &'s QueryState<D, F>,
    ) -> Option<D::Item<'w>> {
        if Self::IS_DENSE {
            loop {
                // we are on the beginning of the query, or finished processing a table, so skip to the next
                if self.current_row == self.current_len {
                    let table_id = self.table_id_iter.next()?;
                    let table = tables.get(*table_id).debug_checked_unwrap();
                    // SAFETY: `table` is from the world that `fetch/filter` were created for,
                    // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
                    D::set_table(&mut self.fetch, &query_state.fetch_state, table);
                    F::set_table(&mut self.filter, &query_state.filter_state, table);
                    self.table_entities = table.entities();
                    self.current_len = table.entity_count();
                    self.current_row = 0;
                    continue;
                }

                // SAFETY: set_table was called prior.
                // `current_row` is a table row in range of the current table, because if it was not, then the if above would have been executed.
                let entity = self.table_entities.get_unchecked(self.current_row);
                let row = TableRow::from_usize(self.current_row);
                if !F::filter_fetch(&mut self.filter, *entity, row) {
                    self.current_row += 1;
                    continue;
                }

                // SAFETY:
                // - set_table was called prior.
                // - `current_row` must be a table row in range of the current table,
                //   because if it was not, then the if above would have been executed.
                // - fetch is only called once for each `entity`.
                let item = D::fetch(&mut self.fetch, *entity, row);

                self.current_row += 1;
                return Some(item);
            }
        } else {
            loop {
                if self.current_row == self.current_len {
                    let archetype_id = self.archetype_id_iter.next()?;
                    let archetype = archetypes.get(*archetype_id).debug_checked_unwrap();
                    let table = tables.get(archetype.table_id()).debug_checked_unwrap();
                    // SAFETY: `archetype` and `tables` are from the world that `fetch/filter` were created for,
                    // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
                    D::set_archetype(&mut self.fetch, &query_state.fetch_state, archetype, table);
                    F::set_archetype(
                        &mut self.filter,
                        &query_state.filter_state,
                        archetype,
                        table,
                    );
                    self.archetype_entities = archetype.entities();
                    self.current_len = archetype.len();
                    self.current_row = 0;
                    continue;
                }

                // SAFETY: set_archetype was called prior.
                // `current_row` is an archetype index row in range of the current archetype, because if it was not, then the if above would have been executed.
                let archetype_entity = self.archetype_entities.get_unchecked(self.current_row);
                if !F::filter_fetch(
                    &mut self.filter,
                    archetype_entity.id(),
                    archetype_entity.table_row(),
                ) {
                    self.current_row += 1;
                    continue;
                }

                // SAFETY:
                // - set_archetype was called prior.
                // - `current_row` must be an archetype index row in range of the current archetype,
                //   because if it was not, then the if above would have been executed.
                // - fetch is only called once for each `archetype_entity`.
                let item = D::fetch(
                    &mut self.fetch,
                    archetype_entity.id(),
                    archetype_entity.table_row(),
                );
                self.current_row += 1;
                return Some(item);
            }
        }
    }
}

pub struct QueryProducer<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    world: &'w World,
    query_state: &'s QueryState<Q, F>,
    last_run: Tick,
    this_run: Tick,
    table_ids: &'s [TableId],
    archetype_ids: &'s [ArchetypeId],
    start_row: usize,
    last_row: usize,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> Debug for QueryProducer<'w, 's, Q, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryProducer")
            .field("table_ids", &self.table_ids)
            .field("archetype_ids", &self.archetype_ids)
            .field("start_row", &self.start_row)
            .field("last_row", &self.last_row)
            .finish()
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QueryProducer<'w, 's, Q, F> {
    const IS_DENSE: bool = Q::IS_DENSE && F::IS_DENSE;

    pub fn new(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        Self {
            world,
            query_state,
            last_run,
            this_run,
            table_ids: &query_state.matched_table_ids,
            archetype_ids: &query_state.matched_archetype_ids,
            start_row: 0,
            last_row: usize::MAX,
        }
    }

    // TODO: this is a straight copy of the safety comment from QueryIter. I think the invariants should be the same
    // but there might be some extra
    /// # Safety
    /// This does not check for mutable query correctness. To be safe, make sure mutable queries
    /// have unique access to the components they query.
    /// This does not validate that `world.id()` matches `query_state.world_id`. Calling this on a `world`
    /// with a mismatched [`WorldId`](crate::world::WorldId) is unsound.
    pub unsafe fn into_iter(self) -> ProducerIter<'w, 's, Q, F> {
        let mut fetch = Q::init_fetch(
            self.world,
            &self.query_state.fetch_state,
            self.last_run,
            self.this_run,
        );
        let mut filter = F::init_fetch(
            self.world,
            &self.query_state.filter_state,
            self.last_run,
            self.this_run,
        );

        if Self::IS_DENSE && self.table_ids.len() == 1 {
            let table = self
                .world
                .storages
                .tables
                .get(self.table_ids[0])
                .debug_checked_unwrap();

            Q::set_table(&mut fetch, &self.query_state.fetch_state, table);
            F::set_table(&mut filter, &self.query_state.filter_state, table);

            let entities = table.entities();
            let length = entities.len();
            return ProducerIter::Simple(QuerySimpleIter {
                fetch,
                filter,
                start_row: self.start_row,
                table_entities_iter: entities
                    [self.start_row.min(length)..self.last_row.min(length)]
                    .iter()
                    .enumerate(),
                archetype_entities_iter: [].iter(), // unused in this branch
                _phantom_data: PhantomData::default(),
            });
        }

        if !Self::IS_DENSE && self.archetype_ids.len() == 1 {
            let archetype = self
                .world
                .archetypes
                .get(self.archetype_ids[0])
                .debug_checked_unwrap();
            let table = self
                .world
                .storages
                .tables
                .get(archetype.table_id())
                .debug_checked_unwrap();

            // SAFETY: `archetype` and `tables` are from the world that `fetch/filter` were created for,
            // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
            Q::set_archetype(&mut fetch, &self.query_state.fetch_state, archetype, table);
            F::set_archetype(
                &mut filter,
                &self.query_state.filter_state,
                archetype,
                table,
            );

            let entities = archetype.entities();
            let length = entities.len();

            return ProducerIter::Simple(QuerySimpleIter {
                fetch,
                filter,
                start_row: self.start_row,
                table_entities_iter: [].iter().enumerate(), // unused in this branch
                archetype_entities_iter: entities
                    [self.start_row.min(length)..self.last_row.min(length)]
                    .iter(),
                _phantom_data: PhantomData::default(),
            });
        }

        let cursor = QueryIterationCursor {
            table_id_iter: self.table_ids.iter(),
            archetype_id_iter: self.archetype_ids.iter(),
            table_entities: &[],
            archetype_entities: &[],
            fetch,
            filter,
            current_len: 0,
            current_row: 0,
            phantom: PhantomData::default(),
        };

        ProducerIter::Multiple(QueryIter {
            query_state: self.query_state,
            tables: &self.world.storages.tables,
            archetypes: &self.world.archetypes,
            cursor,
        })
    }

    pub fn split_at(self, position: usize) -> (Self, Self) {
        if Self::IS_DENSE {
            let (left_table_ids, right_table_ids, left_last_row, right_start_row) =
                self.split_table_ids(position);
            (
                Self {
                    world: self.world,
                    query_state: self.query_state,
                    last_run: self.last_run,
                    this_run: self.this_run,
                    table_ids: left_table_ids,
                    archetype_ids: &[],
                    start_row: self.start_row,
                    last_row: left_last_row,
                },
                Self {
                    world: self.world,
                    query_state: self.query_state,
                    last_run: self.last_run,
                    this_run: self.this_run,
                    table_ids: right_table_ids,
                    archetype_ids: &[],
                    start_row: right_start_row,
                    last_row: self.last_row,
                },
            )
        } else {
            let (left_archetype_ids, right_archetype_ids, left_last_row, right_start_row) =
                self.split_archetype_ids(position);

            (
                Self {
                    world: self.world,
                    query_state: self.query_state,
                    last_run: self.last_run,
                    this_run: self.this_run,
                    table_ids: &[],
                    archetype_ids: left_archetype_ids,
                    start_row: self.start_row,
                    last_row: left_last_row,
                },
                Self {
                    world: self.world,
                    query_state: self.query_state,
                    last_run: self.last_run,
                    this_run: self.this_run,
                    table_ids: &[],
                    archetype_ids: right_archetype_ids,
                    start_row: right_start_row,
                    last_row: self.last_row,
                },
            )
        }
    }

    #[inline]
    fn split_table_ids(&self, position: usize) -> (&'s [TableId], &'s [TableId], usize, usize) {
        let length = self.table_ids.len();
        // if there's only one table
        if length == 1 {
            // split up the table
            let break_row = self.start_row + position;
            (self.table_ids, self.table_ids, break_row, break_row)
        } else {
            // split the list of tables
            let mut sum = 0;
            let table_index = self
                .table_ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    // adjust table counts for start_row and last_row
                    if length == 1 {
                        self.last_row
                            .min(self.world.storages.tables[*id].entity_count())
                            - self.start_row
                    } else if index == 0 {
                        self.world.storages.tables[*id].entity_count() - self.start_row
                    } else if index == length - 1 {
                        self.last_row
                            .min(self.world.storages.tables[*id].entity_count())
                    } else {
                        self.world.storages.tables[*id].entity_count()
                    }
                })
                .position(|count| {
                    sum += count;
                    sum > position
                });

            if let Some(table_index) = table_index {
                // split the list of tables up
                if table_index == 0 {
                    // if the first table is too big, split off
                    // the first table into it's own chuck
                    (
                        &self.table_ids[0..1],
                        &self.table_ids[1..],
                        usize::MAX, // go to the end of the table
                        0,
                    )
                } else {
                    (
                        &self.table_ids[..table_index],
                        &self.table_ids[table_index..],
                        usize::MAX, // go to the end of the table
                        0,
                    )
                }
            } else {
                // this is probably wrong, but I'm not sure it's possible to hit it as
                (self.table_ids, &[], usize::MAX, 0)
            }
        }
    }

    #[inline]
    fn split_archetype_ids(
        &self,
        position: usize,
    ) -> (&'s [ArchetypeId], &'s [ArchetypeId], usize, usize) {
        let length = self.archetype_ids.len();
        // if there's only one table
        if length == 1 {
            // split up the table
            let break_row = self.start_row + position;
            (self.archetype_ids, self.archetype_ids, break_row, break_row)
        } else {
            // split the list of tables
            let mut sum = 0;
            let archetype_index = self
                .archetype_ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    // adjust table counts for start_row and last_row
                    if length == 1 {
                        self.last_row.min(self.world.archetypes[*id].len()) - self.start_row
                    } else if index == 0 {
                        self.world.archetypes[*id].len() - self.start_row
                    } else if index == length - 1 {
                        self.last_row.min(self.world.archetypes[*id].len())
                    } else {
                        self.world.archetypes[*id].len()
                    }
                })
                .position(|count| {
                    sum += count;
                    sum > position
                });

            if let Some(archetype_index) = archetype_index {
                if archetype_index == 0 {
                    // if the first table is too big, split off
                    // the first table into it's own chuck
                    (
                        &self.archetype_ids[0..1],
                        &self.archetype_ids[1..],
                        usize::MAX, // go to the end of the table
                        0,
                    )
                } else {
                    // split the list of tables up
                    (
                        &self.archetype_ids[..archetype_index],
                        &self.archetype_ids[archetype_index..],
                        usize::MAX, // go to the end of the table
                        0,
                    )
                }
            } else {
                // this is probably wrong, but I'm not sure it's possible to hit it as
                (self.archetype_ids, &[], usize::MAX, 0)
            }
        }
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> bevy_tasks::Producer
    for QueryProducer<'w, 's, Q, F>
{
    type Item = Q::Item<'w>;
    type IntoIter = ProducerIter<'w, 's, Q, F>;

    fn into_iter(self) -> Self::IntoIter {
        // todo: write a real safety comment.
        // Safety: this is probably safe...
        unsafe { self.into_iter() }
    }

    fn len(&self) -> usize {
        if Self::IS_DENSE {
            if self.table_ids.len() == 1 {
                self.world.storages.tables[self.table_ids[0]]
                    .entity_count()
                    .min(self.last_row)
                    - self.start_row
            } else {
                self.table_ids
                    .iter()
                    .map(|id| self.world.storages.tables[*id].entity_count())
                    .sum()
            }
        } else if self.archetype_ids.len() == 1 {
            self.world.archetypes[self.archetype_ids[0]]
                .len()
                .min(self.last_row)
                - self.start_row
        } else {
            self.archetype_ids
                .iter()
                .map(|id| self.world.archetypes[*id].len())
                .sum()
        }
    }

    fn split_at(self, position: usize) -> (Self, Self) {
        self.split_at(position)
    }
}

pub enum ProducerIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    Simple(QuerySimpleIter<'w, 's, Q, F>),
    Multiple(QueryIter<'w, 's, Q, F>),
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> Iterator for ProducerIter<'w, 's, Q, F> {
    type Item = Q::Item<'w>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ProducerIter::Simple(inner) => unsafe { inner.next() },
            ProducerIter::Multiple(inner) => inner.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            ProducerIter::Simple(inner) => inner.size_hint(),
            ProducerIter::Multiple(inner) => inner.size_hint(),
        }
    }
}

/// iterator over part of a single table or archetype
pub struct QuerySimpleIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    fetch: Q::Fetch<'w>,
    filter: F::Fetch<'w>,
    start_row: usize,
    table_entities_iter: std::iter::Enumerate<std::slice::Iter<'w, Entity>>,
    archetype_entities_iter: std::slice::Iter<'w, ArchetypeEntity>,
    _phantom_data: PhantomData<&'s ()>,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QuerySimpleIter<'w, 's, Q, F> {
    const IS_DENSE: bool = Q::IS_DENSE && F::IS_DENSE;

    #[inline(always)]
    unsafe fn next(&mut self) -> Option<Q::Item<'w>> {
        if Self::IS_DENSE {
            loop {
                // SAFETY: set_table was called when constructing QuerySimpleIter.
                let (index, entity) = self.table_entities_iter.next()?;

                let row = TableRow::new(index + self.start_row);
                if !F::filter_fetch(&mut self.filter, *entity, row) {
                    continue;
                }

                // SAFETY: set_table was called prior.
                // `current_row` is a table row in range of the current table, because if it was not, then the if above would have been executed.
                return Some(Q::fetch(&mut self.fetch, *entity, row));
            }
        } else {
            loop {
                // SAFETY: set_table was called when constructing QuerySimpleIter.
                let entity = self.archetype_entities_iter.next()?;
                if !F::filter_fetch(&mut self.filter, entity.entity(), entity.table_row()) {
                    continue;
                }

                // SAFETY: set_table was called prior.
                // `current_row` is a table row in range of the current table, because if it was not, then the if above would have been executed.
                return Some(Q::fetch(
                    &mut self.fetch,
                    entity.entity(),
                    entity.table_row(),
                ));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if Self::IS_DENSE {
            self.table_entities_iter.size_hint()
        } else {
            self.archetype_entities_iter.size_hint()
        }
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> Iterator for QuerySimpleIter<'w, 's, Q, F> {
    type Item = Q::Item<'w>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        unsafe { self.next() }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod query_producer {
        use super::*;
        use crate as bevy_ecs;
        use crate::prelude::{Component, With};

        #[derive(Component)]
        struct C;

        #[derive(Component)]
        #[component(storage = "SparseSet")]
        struct S;

        // helper function to check entities id's in a producer
        fn get_entities<F>(producer: QueryProducer<Entity, F>) -> Vec<String>
        where
            F: ReadOnlyWorldQuery,
        {
            // Safety: this is the only active query on the world, so there is no conflicting access
            unsafe {
                producer
                    .into_iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<_>>()
            }
        }

        #[test]
        fn empty_producer() {
            let mut world = World::new();
            let query_state = QueryState::<Entity, With<C>>::new(&mut world);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));

            // Safety: this is the only active query on the world
            assert!(unsafe { producer.into_iter().next().is_none() });
        }

        #[test]
        fn one_item() {
            let mut world = World::new();
            world.spawn(C);

            let query_state = QueryState::<Entity, With<C>>::new(&mut world);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));

            assert_eq!(get_entities(producer), vec!["0v0"]);

            world.spawn(C);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            assert_eq!(get_entities(producer), vec!["0v0", "1v0"]);
        }

        #[test]
        fn split_single_table() {
            let mut world = World::new();
            let mut query_state = QueryState::<Entity, With<C>>::new(&mut world);
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(0);
            assert_eq!(get_entities(left_producer), Vec::<&str>::new());
            assert_eq!(get_entities(right_producer), Vec::<&str>::new());

            world.spawn(C);
            query_state.update_archetypes(&world);

            // splitting at 0 should put all the elements in the right producer
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(0);
            assert_eq!(get_entities(left_producer), Vec::<&str>::new());
            assert_eq!(get_entities(right_producer), vec!["0v0"]);

            // should put all elements in  left producer if position is greater than the length
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(2);
            assert_eq!(get_entities(left_producer), vec!["0v0"]);
            assert_eq!(get_entities(right_producer), Vec::<&str>::new());

            // spawn 2 more entites for a total of 3
            for _ in 0..2 {
                world.spawn(C);
            }

            dbg!(query_state
                .iter(&world)
                .map(|e| format!("{:?}", e))
                .collect::<Vec<_>>());

            // should split at correct position
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(1);
            assert_eq!(get_entities(left_producer), vec!["0v0"]);
            assert_eq!(get_entities(right_producer), vec!["1v0", "2v0"]);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(2);
            assert_eq!(get_entities(left_producer), vec!["0v0", "1v0"]);
            assert_eq!(get_entities(right_producer), vec!["2v0"]);
        }

        #[test]
        fn split_multiple_tables() {
            #[derive(Component)]
            struct D;

            #[derive(Component)]
            struct E;

            let mut world = World::new();

            for _ in 0..3 {
                world.spawn(C);
                world.spawn((C, D));
            }
            world.spawn(D);
            world.spawn((C, E));

            let query_state = QueryState::<Entity, With<C>>::new(&mut world);

            // split on a table boundary
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(3);
            assert_eq!(get_entities(left_producer), vec!["0v0", "2v0", "4v0"]);
            assert_eq!(
                get_entities(right_producer),
                vec!["1v0", "3v0", "5v0", "7v0"]
            );

            // split in the middle of the second table
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(4);
            assert_eq!(get_entities(left_producer), vec!["0v0", "2v0", "4v0"]);
            assert_eq!(
                get_entities(right_producer),
                vec!["1v0", "3v0", "5v0", "7v0"]
            );
        }

        #[test]
        fn split_single_archetype() {
            let mut world = World::new();
            let mut query_state = QueryState::<Entity, With<S>>::new(&mut world);

            world.spawn(S);
            query_state.update_archetypes(&world);

            // splitting at 0 should put all the elements in the right producer
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(0);
            assert_eq!(get_entities(left_producer), Vec::<&str>::new());
            assert_eq!(get_entities(right_producer), vec!["0v0"]);

            // should put all elements in  left producer if position is greater than the length
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(2);
            assert_eq!(get_entities(left_producer), vec!["0v0"]);
            assert_eq!(get_entities(right_producer), Vec::<&str>::new());

            // spawn 2 more entites for a total of 3
            for _ in 0..2 {
                world.spawn(S);
            }

            dbg!(query_state
                .iter(&world)
                .map(|e| format!("{:?}", e))
                .collect::<Vec<_>>());

            // should split at correct position
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(1);
            assert_eq!(get_entities(left_producer), vec!["0v0"]);
            assert_eq!(get_entities(right_producer), vec!["1v0", "2v0"]);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(2);
            assert_eq!(get_entities(left_producer), vec!["0v0", "1v0"]);
            assert_eq!(get_entities(right_producer), vec!["2v0"]);
        }

        #[test]
        fn split_multiple_archetypes() {
            #[derive(Component)]
            struct D;

            #[derive(Component)]
            struct E;

            let mut world = World::new();

            for _ in 0..3 {
                world.spawn(S);
                world.spawn((S, D));
            }
            world.spawn(D);
            world.spawn((S, E));

            let query_state = QueryState::<Entity, With<S>>::new(&mut world);

            // split on a table boundary
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(3);
            assert_eq!(get_entities(left_producer), vec!["0v0", "2v0", "4v0"]);
            assert_eq!(
                get_entities(right_producer),
                vec!["1v0", "3v0", "5v0", "7v0"]
            );

            // split in the middle of the second table
            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(4);
            assert_eq!(get_entities(left_producer), vec!["0v0", "2v0", "4v0"]);
            assert_eq!(
                get_entities(right_producer),
                vec!["1v0", "3v0", "5v0", "7v0"]
            );
        }

        #[test]
        fn split_tables_multiple_times() {
            let mut world = World::new();
            world.spawn_batch((0..10).map(|_| (C)));
            let query_state = QueryState::<Entity, With<C>>::new(&mut world);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(5);

            {
                let (left_producer, right_producer) = left_producer.split_at(2);
                let (left_producer_a, right_producer_a) = left_producer.split_at(1);
                assert_eq!(get_entities(left_producer_a), vec!["0v0"]);
                assert_eq!(get_entities(right_producer_a), vec!["1v0"]);
                let (left_producer_a, right_producer_a) = right_producer.split_at(1);
                assert_eq!(get_entities(left_producer_a), vec!["2v0"]);
                assert_eq!(get_entities(right_producer_a), vec!["3v0", "4v0"]);
            }
            let (left_producer_2, right_producer_2) = right_producer.split_at(2);
            assert_eq!(get_entities(left_producer_2), vec!["5v0", "6v0"]);
            assert_eq!(get_entities(right_producer_2), vec!["7v0", "8v0", "9v0"]);
        }

        #[test]
        fn split_archetypes_multiple_times() {
            let mut world = World::new();
            world.spawn_batch((0..10).map(|_| (S)));
            let query_state = QueryState::<Entity, With<S>>::new(&mut world);

            let producer = QueryProducer::new(&world, &query_state, Tick::new(0), Tick::new(0));
            let (left_producer, right_producer) = producer.split_at(5);

            let (left_producer_1, right_producer_1) = left_producer.split_at(2);
            assert_eq!(get_entities(left_producer_1), vec!["0v0", "1v0"]);
            assert_eq!(get_entities(right_producer_1), vec!["2v0", "3v0", "4v0"]);

            let (left_producer_2, right_producer_2) = right_producer.split_at(2);
            assert_eq!(get_entities(left_producer_2), vec!["5v0", "6v0"]);
            assert_eq!(get_entities(right_producer_2), vec!["7v0", "8v0", "9v0"]);
        }
    }
}
