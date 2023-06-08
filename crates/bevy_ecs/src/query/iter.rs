use crate::{
    archetype::{ArchetypeEntity, ArchetypeId, Archetypes},
    component::Tick,
    entity::{Entities, Entity},
    prelude::World,
    query::{ArchetypeFilter, DebugCheckedUnwrap, QueryState, WorldQuery},
    storage::{TableId, TableRow, Tables},
};
use core::fmt::Debug;
use std::{borrow::Borrow, iter::FusedIterator, marker::PhantomData, mem::MaybeUninit};

use super::ReadOnlyWorldQuery;

/// An [`Iterator`] over query results of a [`Query`](crate::system::Query).
///
/// This struct is created by the [`Query::iter`](crate::system::Query::iter) and
/// [`Query::iter_mut`](crate::system::Query::iter_mut) methods.
pub struct QueryIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    query_state: &'s QueryState<Q, F>,
    cursor: QueryIterationCursor<'w, 's, Q, F>,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QueryIter<'w, 's, Q, F> {
    /// # Safety
    /// This does not check for mutable query correctness. To be safe, make sure mutable queries
    /// have unique access to the components they query.
    /// This does not validate that `world.id()` matches `query_state.world_id`. Calling this on a `world`
    /// with a mismatched [`WorldId`](crate::world::WorldId) is unsound.
    pub(crate) unsafe fn new(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        QueryIter {
            query_state,
            tables: &world.storages().tables,
            archetypes: &world.archetypes,
            cursor: QueryIterationCursor::init(world, query_state, last_run, this_run),
        }
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> Iterator for QueryIter<'w, 's, Q, F> {
    type Item = Q::Item<'w>;

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
        let archetype_query = Q::IS_ARCHETYPAL && F::IS_ARCHETYPAL;
        let min_size = if archetype_query { max_size } else { 0 };
        (min_size, Some(max_size))
    }
}

// This is correct as [`QueryIter`] always returns `None` once exhausted.
impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> FusedIterator for QueryIter<'w, 's, Q, F> {}

/// An [`Iterator`] over the query items generated from an iterator of [`Entity`]s.
///
/// Items are returned in the order of the provided iterator.
/// Entities that don't match the query are skipped.
///
/// This struct is created by the [`Query::iter_many`](crate::system::Query::iter_many) and [`Query::iter_many_mut`](crate::system::Query::iter_many_mut) methods.
pub struct QueryManyIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery, I: Iterator>
where
    I::Item: Borrow<Entity>,
{
    entity_iter: I,
    entities: &'w Entities,
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    fetch: Q::Fetch<'w>,
    filter: F::Fetch<'w>,
    query_state: &'s QueryState<Q, F>,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery, I: Iterator> QueryManyIter<'w, 's, Q, F, I>
where
    I::Item: Borrow<Entity>,
{
    /// # Safety
    /// This does not check for mutable query correctness. To be safe, make sure mutable queries
    /// have unique access to the components they query.
    /// This does not validate that `world.id()` matches `query_state.world_id`. Calling this on a `world`
    /// with a mismatched [`WorldId`](crate::world::WorldId) is unsound.
    pub(crate) unsafe fn new<EntityList: IntoIterator<IntoIter = I>>(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        entity_list: EntityList,
        last_run: Tick,
        this_run: Tick,
    ) -> QueryManyIter<'w, 's, Q, F, I> {
        let fetch = Q::init_fetch(world, &query_state.fetch_state, last_run, this_run);
        let filter = F::init_fetch(world, &query_state.filter_state, last_run, this_run);
        QueryManyIter {
            query_state,
            entities: &world.entities,
            archetypes: &world.archetypes,
            tables: &world.storages.tables,
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
    unsafe fn fetch_next_aliased_unchecked(&mut self) -> Option<Q::Item<'w>> {
        for entity in self.entity_iter.by_ref() {
            let entity = *entity.borrow();
            let location = match self.entities.get(entity) {
                Some(location) => location,
                None => continue,
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
            Q::set_archetype(
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
                // SAFETY: set_archetype was called prior, `location.archetype_row` is an archetype index in range of the current archetype
                return Some(Q::fetch(&mut self.fetch, entity, location.table_row));
            }
        }
        None
    }

    /// Get next result from the query
    #[inline(always)]
    pub fn fetch_next(&mut self) -> Option<Q::Item<'_>> {
        // SAFETY: we are limiting the returned reference to self,
        // making sure this method cannot be called multiple times without getting rid
        // of any previously returned unique references first, thus preventing aliasing.
        unsafe { self.fetch_next_aliased_unchecked().map(Q::shrink) }
    }
}

impl<'w, 's, Q: ReadOnlyWorldQuery, F: ReadOnlyWorldQuery, I: Iterator> Iterator
    for QueryManyIter<'w, 's, Q, F, I>
where
    I::Item: Borrow<Entity>,
{
    type Item = Q::Item<'w>;

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
impl<'w, 's, Q: ReadOnlyWorldQuery, F: ReadOnlyWorldQuery, I: Iterator> FusedIterator
    for QueryManyIter<'w, 's, Q, F, I>
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
pub struct QueryCombinationIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery, const K: usize> {
    tables: &'w Tables,
    archetypes: &'w Archetypes,
    query_state: &'s QueryState<Q, F>,
    cursors: [QueryIterationCursor<'w, 's, Q, F>; K],
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery, const K: usize>
    QueryCombinationIter<'w, 's, Q, F, K>
{
    /// # Safety
    /// This does not check for mutable query correctness. To be safe, make sure mutable queries
    /// have unique access to the components they query.
    /// This does not validate that `world.id()` matches `query_state.world_id`. Calling this on a
    /// `world` with a mismatched [`WorldId`](crate::world::WorldId) is unsound.
    pub(crate) unsafe fn new(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        // Initialize array with cursors.
        // There is no FromIterator on arrays, so instead initialize it manually with MaybeUninit

        let mut array: MaybeUninit<[QueryIterationCursor<'w, 's, Q, F>; K]> = MaybeUninit::uninit();
        let ptr = array
            .as_mut_ptr()
            .cast::<QueryIterationCursor<'w, 's, Q, F>>();
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
            tables: &world.storages().tables,
            archetypes: &world.archetypes,
            cursors: array.assume_init(),
        }
    }

    /// Safety:
    /// The lifetime here is not restrictive enough for Fetch with &mut access,
    /// as calling `fetch_next_aliased_unchecked` multiple times can produce multiple
    /// references to the same component, leading to unique reference aliasing.
    ///.
    /// It is always safe for shared access.
    unsafe fn fetch_next_aliased_unchecked(&mut self) -> Option<[Q::Item<'w>; K]> {
        if K == 0 {
            return None;
        }

        // PERF: can speed up the following code using `cursor.remaining()` instead of `next_item.is_none()`
        // when Q::IS_ARCHETYPAL && F::IS_ARCHETYPAL
        //
        // let `i` be the index of `c`, the last cursor in `self.cursors` that
        // returns `K-i` or more elements.
        // Make cursor in index `j` for all `j` in `[i, K)` a copy of `c` advanced `j-i+1` times.
        // If no such `c` exists, return `None`
        'outer: for i in (0..K).rev() {
            match self.cursors[i].next(self.tables, self.archetypes, self.query_state) {
                Some(_) => {
                    for j in (i + 1)..K {
                        self.cursors[j] = self.cursors[j - 1].clone_cursor();
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

        let mut values = MaybeUninit::<[Q::Item<'w>; K]>::uninit();

        let ptr = values.as_mut_ptr().cast::<Q::Item<'w>>();
        for (offset, cursor) in self.cursors.iter_mut().enumerate() {
            ptr.add(offset).write(cursor.peek_last().unwrap());
        }

        Some(values.assume_init())
    }

    /// Get next combination of queried components
    #[inline]
    pub fn fetch_next(&mut self) -> Option<[Q::Item<'_>; K]> {
        // SAFETY: we are limiting the returned reference to self,
        // making sure this method cannot be called multiple times without getting rid
        // of any previously returned unique references first, thus preventing aliasing.
        unsafe {
            self.fetch_next_aliased_unchecked()
                .map(|array| array.map(Q::shrink))
        }
    }
}

// Iterator type is intentionally implemented only for read-only access.
// Doing so for mutable references would be unsound, because calling `next`
// multiple times would allow multiple owned references to the same data to exist.
impl<'w, 's, Q: ReadOnlyWorldQuery, F: ReadOnlyWorldQuery, const K: usize> Iterator
    for QueryCombinationIter<'w, 's, Q, F, K>
{
    type Item = [Q::Item<'w>; K];

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

        let archetype_query = F::IS_ARCHETYPAL && Q::IS_ARCHETYPAL;
        let known_max = max_combinations.unwrap_or(usize::MAX);
        let min_combinations = if archetype_query { known_max } else { 0 };
        (min_combinations, max_combinations)
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> ExactSizeIterator for QueryIter<'w, 's, Q, F>
where
    F: ArchetypeFilter,
{
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

// This is correct as [`QueryCombinationIter`] always returns `None` once exhausted.
impl<'w, 's, Q: ReadOnlyWorldQuery, F: ReadOnlyWorldQuery, const K: usize> FusedIterator
    for QueryCombinationIter<'w, 's, Q, F, K>
{
}

struct QueryIterationCursor<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    table_id_iter: std::slice::Iter<'s, TableId>,
    archetype_id_iter: std::slice::Iter<'s, ArchetypeId>,
    fetch: Q::Fetch<'w>,
    filter: F::Fetch<'w>,
    cached_iter: QuerySimpleIter<'w, 's, Q, F>,
    phantom: PhantomData<Q>,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QueryIterationCursor<'w, 's, Q, F> {
    /// This function is safe to call if `(Q, F): ReadOnlyWorldQuery` holds.
    ///
    /// # Safety
    /// While calling this method on its own cannot cause UB it is marked `unsafe` as the caller must ensure
    /// that the returned value is not used in any way that would cause two `QueryItem<Q>` for the same
    /// `archetype_row` or `table_row` to be alive at the same time.
    unsafe fn clone_cursor(&self) -> Self {
        Self {
            table_id_iter: self.table_id_iter.clone(),
            archetype_id_iter: self.archetype_id_iter.clone(),
            // SAFETY: upheld by caller invariants
            fetch: Q::clone_fetch(&self.fetch),
            filter: F::clone_fetch(&self.filter),
            cached_iter: QuerySimpleIter {
                // SAFETY: upheld by caller invariants
                fetch: Q::clone_fetch(&self.fetch),
                filter: F::clone_fetch(&self.filter),
                start_row: 0,
                table_entities_iter: self.cached_iter.table_entities_iter.clone(),
                archetype_entities_iter: self.cached_iter.archetype_entities_iter.clone(),
                last_entity: Entity::PLACEHOLDER,
                last_row: 0,
                last_archetype_entity: None,
                _phantom_data: PhantomData,
            },
            phantom: PhantomData,
        }
    }
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QueryIterationCursor<'w, 's, Q, F> {
    const IS_DENSE: bool = Q::IS_DENSE && F::IS_DENSE;

    unsafe fn init_empty(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        QueryIterationCursor {
            table_id_iter: [].iter(),
            archetype_id_iter: [].iter(),
            ..Self::init(world, query_state, last_run, this_run)
        }
    }

    unsafe fn init(
        world: &'w World,
        query_state: &'s QueryState<Q, F>,
        last_run: Tick,
        this_run: Tick,
    ) -> Self {
        let fetch = Q::init_fetch(world, &query_state.fetch_state, last_run, this_run);
        let filter = F::init_fetch(world, &query_state.filter_state, last_run, this_run);
        QueryIterationCursor {
            fetch: Q::clone_fetch(&fetch),
            filter: F::clone_fetch(&filter),
            table_id_iter: query_state.matched_table_ids.iter(),
            archetype_id_iter: query_state.matched_archetype_ids.iter(),
            cached_iter: QuerySimpleIter::init_empty(fetch, filter),
            phantom: PhantomData,
        }
    }

    /// retrieve item returned from most recent `next` call again.
    #[inline]
    unsafe fn peek_last(&mut self) -> Option<Q::Item<'w>> {
        self.cached_iter.peek_last()
        // if self.current_row > 0 {
        //     let index = self.current_row - 1;
        //     if Self::IS_DENSE {
        //         let entity = self.table_entities.get_unchecked(index);
        //         Some(Q::fetch(&mut self.fetch, *entity, TableRow::new(index)))
        //     } else {
        //         let archetype_entity = self.archetype_entities.get_unchecked(index);
        //         Some(Q::fetch(
        //             &mut self.fetch,
        //             archetype_entity.entity(),
        //             archetype_entity.table_row(),
        //         ))
        //     }
        // } else {
        //     None
        // }
    }

    /// How many values will this cursor return at most?
    ///
    /// Note that if `Q::IS_ARCHETYPAL && F::IS_ARCHETYPAL`, the return value
    /// will be **the exact count of remaining values**.
    fn max_remaining(&self, tables: &'w Tables, archetypes: &'w Archetypes) -> usize {
        let remaining_matched: usize = if Self::IS_DENSE {
            let ids = self.table_id_iter.clone();
            ids.map(|id| tables[*id].entity_count()).sum()
        } else {
            let ids = self.archetype_id_iter.clone();
            ids.map(|id| archetypes[*id].len()).sum()
        };
        remaining_matched + self.cached_iter.size_hint().1.unwrap()
    }

    // NOTE: If you are changing query iteration code, remember to update the following places, where relevant:
    // QueryIter, QueryIterationCursor, QueryManyIter, QueryCombinationIter, QueryState::for_each_unchecked_manual, QueryState::par_for_each_unchecked_manual
    /// # Safety
    /// `tables` and `archetypes` must belong to the same world that the [`QueryIterationCursor`]
    /// was initialized for.
    /// `query_state` must be the same [`QueryState`] that was passed to `init` or `init_empty`.
    #[inline(always)]
    unsafe fn next(
        &mut self,
        tables: &'w Tables,
        archetypes: &'w Archetypes,
        query_state: &'s QueryState<Q, F>,
    ) -> Option<Q::Item<'w>> {
        if Self::IS_DENSE {
            loop {
                let item = self.cached_iter.next();
                // we are on the beginning of the query, or finished processing a table, so skip to the next
                if item.is_none() {
                    let table_id = self.table_id_iter.next()?;
                    let table = tables.get(*table_id).debug_checked_unwrap();
                    // SAFETY: `table` is from the world that `fetch/filter` were created for,
                    // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
                    Q::set_table(&mut self.fetch, &query_state.fetch_state, table);
                    F::set_table(&mut self.filter, &query_state.filter_state, table);

                    self.cached_iter = QuerySimpleIter {
                        fetch: Q::clone_fetch(&self.fetch),
                        filter: F::clone_fetch(&self.filter),
                        start_row: 0,
                        table_entities_iter: table.entities().iter().enumerate(),
                        archetype_entities_iter: [].iter(), // unused in this branch
                        last_entity: Entity::PLACEHOLDER,
                        last_row: 0,
                        last_archetype_entity: None,
                        _phantom_data: PhantomData::default(),
                    };

                    continue;
                }

                return item;
            }
        } else {
            loop {
                let item = self.cached_iter.next();

                if item.is_none() {
                    let archetype_id = self.archetype_id_iter.next()?;
                    let archetype = archetypes.get(*archetype_id).debug_checked_unwrap();
                    // SAFETY: `archetype` and `tables` are from the world that `fetch/filter` were created for,
                    // `fetch_state`/`filter_state` are the states that `fetch/filter` were initialized with
                    let table = tables.get(archetype.table_id()).debug_checked_unwrap();
                    Q::set_archetype(&mut self.fetch, &query_state.fetch_state, archetype, table);
                    F::set_archetype(
                        &mut self.filter,
                        &query_state.filter_state,
                        archetype,
                        table,
                    );

                    self.cached_iter = QuerySimpleIter {
                        fetch: Q::clone_fetch(&self.fetch),
                        filter: F::clone_fetch(&self.filter),
                        start_row: 0,
                        table_entities_iter: [].iter().enumerate(),
                        archetype_entities_iter: archetype.entities().iter(), // unused in this branch
                        last_entity: Entity::PLACEHOLDER,
                        last_row: 0,
                        last_archetype_entity: None,
                        _phantom_data: PhantomData::default(),
                    };

                    continue;
                }

                return item;
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
                last_entity: Entity::PLACEHOLDER,
                last_row: 0,
                last_archetype_entity: None,
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
                last_entity: Entity::PLACEHOLDER,
                last_row: 0,
                last_archetype_entity: None,
                _phantom_data: PhantomData::default(),
            });
        }

        let cursor = QueryIterationCursor {
            table_id_iter: self.table_ids.iter(),
            archetype_id_iter: self.archetype_ids.iter(),
            fetch: Q::clone_fetch(&fetch),
            filter: F::clone_fetch(&filter),
            cached_iter: QuerySimpleIter::init_empty(fetch, filter),
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

/// iterator over part or whole of a single table or archetype
pub struct QuerySimpleIter<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> {
    fetch: Q::Fetch<'w>,
    filter: F::Fetch<'w>,
    start_row: usize,
    table_entities_iter: std::iter::Enumerate<std::slice::Iter<'w, Entity>>,
    archetype_entities_iter: std::slice::Iter<'w, ArchetypeEntity>,
    last_entity: Entity,
    last_row: usize,
    last_archetype_entity: Option<ArchetypeEntity>,
    _phantom_data: PhantomData<&'s ()>,
}

impl<'w, 's, Q: WorldQuery, F: ReadOnlyWorldQuery> QuerySimpleIter<'w, 's, Q, F> {
    const IS_DENSE: bool = Q::IS_DENSE && F::IS_DENSE;

    pub fn init_empty(fetch: Q::Fetch<'w>, filter: F::Fetch<'w>) -> Self {
        Self {
            fetch,
            filter,
            start_row: 0,
            table_entities_iter: [].iter().enumerate(),
            archetype_entities_iter: [].iter(),
            last_entity: Entity::PLACEHOLDER,
            last_row: 0,
            last_archetype_entity: None,
            _phantom_data: PhantomData::default(),
        }
    }

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

                self.last_entity = *entity;
                self.last_row = index + self.start_row;
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
        // the minimum number of elements is zero since all entities
        // could be filtered out
        if Self::IS_DENSE {
            (0, self.table_entities_iter.size_hint().1)
        } else {
            (0, self.archetype_entities_iter.size_hint().1)
        }
    }

    // TODO: not dense branch won't work right as cannot set the archetype entity. should find a way
    // for combinations iter to work around this somehow. instead of pushing resposibility onto simple iter
    unsafe fn peek_last(&mut self) -> Option<Q::Item<'w>> {
        if Self::IS_DENSE {
            if self.last_entity != Entity::PLACEHOLDER {
                Some(Q::fetch(
                    &mut self.fetch,
                    self.last_entity,
                    TableRow::new(self.last_row),
                ))
            } else {
                None
            }
        } else {
            if let Some(ref entity) = self.last_archetype_entity {
                Some(Q::fetch(
                    &mut self.fetch,
                    entity.entity(),
                    entity.table_row(),
                ))
            } else {
                None
            }
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
