use std::cell::Cell;

use thread_local::ThreadLocal;

#[derive(Default)]
pub struct ThreadLocalVec<T: Send> {
    inner: ThreadLocal<Cell<Vec<T>>>,
}

impl<T: Send> ThreadLocalVec<T> {
    pub fn new() -> Self {
        ThreadLocalVec {
            inner: ThreadLocal::new(),
        }
    }

    pub fn push(&self, item: T) {
        let cell = self.inner.get_or_default();
        let mut vec = cell.take();
        vec.push(item);
        cell.set(vec);
    }

    pub fn into_iter(self) -> impl Iterator<Item = T> {
        self.inner.into_iter().flat_map(|cell| cell.take())
    }
}
