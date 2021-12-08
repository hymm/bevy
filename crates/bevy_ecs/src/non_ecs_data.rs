use crate::storage::SparseSetIndex;

pub const NON_SEND_DATA_ID: usize = 0;
pub const ARCHETYPES_DATA_ID: usize = 1;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct NonEcsDataId(usize);

impl NonEcsDataId {
    #[inline]
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0
    }
}

impl SparseSetIndex for NonEcsDataId {
    #[inline]
    fn sparse_set_index(&self) -> usize {
        self.0
    }

    fn get_sparse_set_index(value: usize) -> Self {
        Self(value)
    }
}
