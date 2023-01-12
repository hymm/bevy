use crate::Scope;

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
    batch_size: usize,
) where
    'env: 'scope,
    P: Producer<Item = T> + 'scope,
    F: FnOnce(T) + Send + Sync + Copy + 'scope,
{
    let length = producer.len();
    if length > batch_size {
        let mid = length / 2;
        let (left_producer, right_producer) = producer.split_at(mid);
        join_tasks(
            scope,
            move || execute_operation(scope, op, left_producer, batch_size),
            move || execute_operation(scope, op, right_producer, batch_size),
        );
    } else {
        for item in producer.into_iter() {
            op(item);
        }
    }
}

/// trait for using [`execute_operation`]
pub trait Producer: Send + Sized {
    /// type used by [`execute_operation`]
    type Item;

    /// The type of iterator we will become.
    type IntoIter: Iterator<Item = Self::Item> + DoubleEndedIterator + ExactSizeIterator;

    /// Convert `self` into an iterator; at this point, no more parallel splits
    /// are possible.
    fn into_iter(self) -> Self::IntoIter;

    /// number of items in producer
    fn len(&self) -> usize;

    /// split producer into 2 Producers at position
    fn split_at(self, position: usize) -> (Self, Self);
}
