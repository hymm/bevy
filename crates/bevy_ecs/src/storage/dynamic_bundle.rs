use bevy_ptr::OwningPtr;
use bevy_utils::{hashbrown::hash_map::Entry, TypeIdMap};
use std::{
    alloc::{alloc, dealloc, Layout},
    any::TypeId,
    ptr::NonNull,
};

use crate::{component::ComponentDescriptor, prelude::Component};

/// Data storage for moving data outside of the world.
pub struct DynamicBundle {
    data: NonNull<u8>,
    /// (ConponentInfo, offset)
    // Note to self: we need ComponentInfo because it stores the drop info
    info: Vec<(ComponentDescriptor, usize)>,
    layout: Layout,
    /// offset to next free byte of `data`
    cursor: usize,
    /// Map a component id to the index
    indices: TypeIdMap<usize>,
}

impl DynamicBundle {
    /// allocate with size `capacity`
    pub fn capacity(size: usize) -> DynamicBundle {
        // Safety: Layout is non zero sized.
        let ptr = unsafe { alloc(Layout::from_size_align(size, 1).unwrap()) };
        assert!(ptr != std::ptr::null_mut());
        DynamicBundle {
            // Safety: alloc only returns null if the allocation fails
            data: unsafe { NonNull::new_unchecked(ptr) },
            info: vec![],
            layout: Layout::from_size_align(0, 8).unwrap(),
            cursor: 0,
            indices: TypeIdMap::default(),
        }
    }

    /// Clear the DynmaicBuffer. This allows the allocation to be reused.
    pub fn clear(&mut self) {
        self.indices.clear();
        self.info.clear();
        self.cursor = 0;

        // TODO: we need to call the drop impl's of any stored data
    }

    /// insert `data` into the [`DynamicBundle`]. If the data already exists replace the old value
    pub fn add<T: 'static + Component>(&mut self, mut data: T) {
        match self.indices.entry(TypeId::of::<T>()) {
            Entry::Occupied(occupied_entry) => {
                // if the data already exists we need to replace the value
                let index = *occupied_entry.get();
                let (desc, offset) = &mut self.info[index];

                // Safety: `offset` is garunteed to be in `data`
                let ptr = unsafe { self.data.byte_add(*offset) };
                if let Some(drop) = desc.drop {
                    // SAFETY: ptr is of the corrent type
                    unsafe {
                        drop(OwningPtr::new(ptr));
                    }
                }

                unsafe {
                    std::ptr::copy_nonoverlapping(&mut data as *mut T, ptr.as_ptr() as *mut T, 1);
                }
            }
            Entry::Vacant(vacant) => {
                let layout = Layout::new::<T>();
                let offset = align(self.cursor, layout.align());
                // unsafe { self.data.byte_add(self.cursor) }.align_offset(dbg!(layout.align()));
                let end = offset + self.cursor + layout.size();
                // if we don't have enough capacity or the alignment has changed we need to reallocate
                // Note: As long as the alignment only increases the data already placed in the allocation
                // will be aligned for their own layouts. i.e. increasing the alignment will not make the
                // data already written to become unaligned.
                if end > self.layout.size() || self.layout.align() < layout.align() {
                    let new_align = self.layout.align().max(layout.align());
                    let (new_ptr, new_layout) =
                        Self::grow(self.data, self.cursor, self.layout, end, new_align);
                    self.data = new_ptr;
                    self.layout = new_layout;
                }

                let addr = unsafe { self.data.as_ptr().add(offset) };
                unsafe { std::ptr::copy_nonoverlapping(&mut data as *mut T, addr as *mut T, 1) };

                vacant.insert(self.info.len());
                self.info.push((ComponentDescriptor::new::<T>(), offset));
                self.cursor = end;
            }
        }
        // leak the data so drop is not called
        core::mem::forget(data);
    }

    /// grow the currrent allocation, returns the new size
    fn grow(
        old_ptr: NonNull<u8>,
        cursor: usize,
        old_layout: Layout,
        new_min_capacity: usize,
        align: usize,
    ) -> (NonNull<u8>, Layout) {
        let layout =
            Layout::from_size_align(new_min_capacity.next_power_of_two().max(64), align).unwrap();
        // SAFETY: max check above will always return a nonzero size
        let new_ptr = unsafe { alloc(layout) };
        assert!(new_ptr != std::ptr::null_mut(), "allocation failed");
        // SAFETY: asserted that the ptr was not null above
        let new_ptr = unsafe { NonNull::new_unchecked(new_ptr) };
        if old_layout.size() > 0 {
            // SAFETY:
            // * the pointers are from different allocations as so cannot overlap
            // * cursor is always within the different allocations
            unsafe { std::ptr::copy_nonoverlapping(old_ptr.as_ptr(), new_ptr.as_ptr(), cursor) };
            unsafe { dealloc(old_ptr.as_ptr(), old_layout) };
        }
        (new_ptr, layout)
    }
}

impl Default for DynamicBundle {
    fn default() -> DynamicBundle {
        DynamicBundle {
            data: NonNull::dangling(),
            info: vec![],
            layout: Layout::from_size_align(0, 8).unwrap(),
            cursor: 0,
            indices: TypeIdMap::default(),
        }
    }
}

impl Drop for DynamicBundle {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            // TODO: we need to call the drop impl's of the stored values.
            for (desc, offset) in &mut self.info {
                let ptr = unsafe { self.data.add(*offset) };
                let Some(drop) = desc.drop else {
                    continue;
                };
                // SAFETY: ptr is aligned and is valid until deallocated below
                let ptr = unsafe { OwningPtr::new(ptr) };
                // SAFETY: ptr points to the same type as desc
                unsafe { drop(ptr) };
            }

            // Safety:
            // * layout matches what it was allocated with
            // * points to a valid allocation that was allocated from the global allocator
            unsafe { dealloc(self.data.as_ptr(), self.layout) };
        }
    }
}

fn align(x: usize, alignment: usize) -> usize {
    debug_assert!(alignment.is_power_of_two());
    (x + alignment - 1) & (!alignment + 1)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use crate as bevy_ecs;
    use bevy_ecs_macros::Component;

    #[derive(Component, Clone, Debug)]
    struct DropCk(Arc<AtomicUsize>);
    impl DropCk {
        fn new_pair() -> (Self, Arc<AtomicUsize>) {
            let atomic = Arc::new(AtomicUsize::new(0));
            (DropCk(atomic.clone()), atomic)
        }
    }

    impl Drop for DropCk {
        fn drop(&mut self) {
            self.0.as_ref().fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn calls_drop_if_exists() {
        let (component, counter) = DropCk::new_pair();
        {
            let mut bundle = DynamicBundle::default();
            bundle.add(component);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn calls_drop_if_replaced() {
        let (component, counter) = DropCk::new_pair();

        let mut bundle = DynamicBundle::default();
        bundle.add(component);
        let (new_component, _new_counter) = DropCk::new_pair();
        bundle.add(new_component);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
