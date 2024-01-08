use crate::Scope;
use core::fmt::Debug;

/// this function runs one task on the current thread and spawns the second task on the scope
fn join_tasks<'scope, 'env, A, B>(scope: &'scope Scope<'scope, 'env, ()>, task_a: A, task_b: B)
where
    A: FnOnce() + Send + 'scope,
    B: FnOnce() + Send + 'scope,
{
    scope.spawn(async move {
        task_b();
    });

    task_a();
}

/// executor an operation on all items of producer using scope
pub fn execute_operation<'scope, 'env, F, P, T>(
    scope: &'scope Scope<'scope, 'env, ()>,
    op: F,
    producer: P,
    length: usize,
    batch_size: usize,
) where
    'env: 'scope,
    P: Producer<Item = T> + Debug + 'scope,
    F: Fn(T) + Send + Sync + Clone + 'scope,
{
    if length > batch_size {
        let mid = length / 2;
        let (left_producer, right_producer) = producer.split_at(mid);
        let left_length = left_producer.len();
        let right_length = right_producer.len();
        let op_a = op.clone();
        join_tasks(
            scope,
            move || execute_operation(scope, op_a, left_producer, left_length, batch_size),
            move || execute_operation(scope, op, right_producer, right_length, batch_size),
        );
    } else {
        #[cfg(feature = "trace")]
        let _span = tracing::info_span!(
            "op",
            item = std::any::type_name::<<P as Producer>::Item>(),
            length = producer.len(),
        )
        .entered();

        producer.into_iter().for_each(op);
    }
}

/// trait for using [`execute_operation`]
pub trait Producer: Send + Sized {
    /// type used by [`execute_operation`]
    type Item;

    /// The type of iterator we will become.
    type IntoIter: Iterator<Item = Self::Item>;

    /// Convert `self` into an iterator; at this point, no more parallel splits
    /// are possible.
    fn into_iter(self) -> Self::IntoIter;

    /// number of items in producer
    fn len(&self) -> usize;

    /// no items in producer
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// split producer into 2 Producers at position
    fn split_at(self, position: usize) -> (Self, Self);
}
