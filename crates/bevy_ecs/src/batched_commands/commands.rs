use core::marker::PhantomData;

use crate::{bundle::Bundle, component::StorageType, entity::Entity};
use bevy_ptr::{MovingPtr, OwningPtr};
use lender_dyn::{Lend, LendingIterator};
use no_alloc::{boxed_s, BoxS};

pub trait BatchedCommand {
    fn entity(&self) -> Entity;

    /// Safety:
    /// * memory referenced to bundle must not be dereferenced after.
    // TODO: functions with generics cannot be function pointers, we eventually need to type erase
    // this so it might be an issue.
    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    >;
}

pub struct Spawn<B: Bundle> {
    entity: Entity,
    bundle: B,
}
impl<B: Bundle> BatchedCommand for Spawn<B> {
    fn entity(&self) -> Entity {
        self.entity
    }

    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    > {
        let bundle = &mut self.bundle;
        let bundle = unsafe { MovingPtr::new(bundle.into()) };
        boxed_s!(B::get_components(bundle))
    }
}

pub struct Insert<B: Bundle> {
    entity: Entity,
    bundle: B,
}
impl<B: Bundle> BatchedCommand for Insert<B> {
    fn entity(&self) -> Entity {
        self.entity
    }

    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    > {
        let bundle = &mut self.bundle;
        let bundle = unsafe { MovingPtr::new(bundle.into()) };
        boxed_s!(B::get_components(bundle))
    }
}

pub struct Remove<B: Bundle> {
    entity: Entity,
    bundle: PhantomData<B>,
}
impl<B: Bundle> BatchedCommand for Remove<B> {
    fn entity(&self) -> Entity {
        self.entity
    }

    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    > {
        boxed_s!(lender_dyn::empty::empty())
    }
}

pub struct Modify<Add: Bundle, Remove: Bundle> {
    entity: Entity,
    add: Add,
    remove: PhantomData<Remove>,
}
impl<Add: Bundle, Remove: Bundle> BatchedCommand for Modify<Add, Remove> {
    fn entity(&self) -> Entity {
        self.entity
    }

    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    > {
        let bundle = &mut self.add;
        let bundle = unsafe { MovingPtr::new(bundle.into()) };
        boxed_s!(Add::get_components(bundle))
    }
}

pub struct Despawn {
    entity: Entity,
}
impl BatchedCommand for Despawn {
    fn entity(&self) -> Entity {
        self.entity
    }

    unsafe fn drain<'a>(
        &'a mut self,
    ) -> BoxS<
        dyn LendingIterator<Lend = dyn for<'all> Lend<'all, Item = (StorageType, OwningPtr<'a>)>>
            + 'a,
        [usize; 1],
    > {
        boxed_s!(lender_dyn::empty::empty())
    }
}
