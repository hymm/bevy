use crate::{
  archetype::ArchetypeComponentId,
  query::Access,
};
use async_broadcast::{broadcast, Receiver, Sender};
use async_mutex::Mutex;
use dashmap::DashMap;
use std::sync::Arc;

pub struct SharedSystemAccess {
  access: Arc<Mutex<Access<ArchetypeComponentId>>>,
  active_access: Arc<DashMap<usize, Access<ArchetypeComponentId>>>,
  access_updated_recv: Receiver<()>,
  access_updated_send: Sender<()>,
}

impl SharedSystemAccess {
  pub async fn wait_for_access(&mut self, other: &Access<ArchetypeComponentId>, index: usize) {
      loop {
          {
              let mut access = self.access.lock().await;
              if access.is_compatible(other) {
                  access.extend(other);
                  self.active_access.insert(index, other.clone());
                  break;
              }
          }
          self.access_updated_recv.recv().await.unwrap();
      }
  }

  // use when system is finished running
  pub async fn remove_access(&mut self, access_id: usize) {
      {
          let mut access = self.access.lock().await;
          access.clear();
          self.active_access.remove(&access_id);
          self.active_access
              .iter()
              .for_each(|active_access| access.extend(&active_access));
      }

      self.access_updated_send.broadcast(()).await.unwrap();
  }
}

impl Clone for SharedSystemAccess {
  fn clone(&self) -> Self {
      Self {
          access: self.access.clone(),
          active_access: self.active_access.clone(),
          access_updated_recv: self.access_updated_recv.clone(),
          access_updated_send: self.access_updated_send.clone(),
      }
  }
}

impl Default for SharedSystemAccess {
  fn default() -> Self {
      let (mut access_updated_send, access_updated_recv) = broadcast(1);
      access_updated_send.set_overflow(true);

      Self {
          access: Default::default(),
          active_access: Default::default(),
          access_updated_recv,
          access_updated_send,
      }
  }
}