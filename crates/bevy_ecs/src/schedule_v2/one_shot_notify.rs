use event_listener::{Event, EventListener};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::{Context, Poll};

#[derive(Default)]
struct Channel {
    notified: Arc<AtomicBool>,
    notify_event: Event,
}

pub fn one_shot_notify() -> (Sender, Receiver) {
    let channel = Arc::new(Channel::default());

    let s = Sender {
        channel: channel.clone(),
    };

    let r = Receiver { channel };

    (s, r)
}

#[derive(Clone)]
pub struct Sender {
    channel: Arc<Channel>,
}

impl Sender {
    pub fn notify(&mut self) {
        self.channel.notified.store(true, Ordering::Relaxed);
        self.channel.notify_event.notify(usize::MAX);
    }

    pub fn reset(&mut self) {
        self.channel.notified.store(false, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct Receiver {
    channel: Arc<Channel>,
}

impl Receiver {
    fn get_notified(&self) -> bool {
        self.channel.notified.load(Ordering::Relaxed)
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            receiver: self,
            listener: None,
        }
    }
}

pub struct Notified<'a> {
    receiver: &'a Receiver,
    listener: Option<EventListener>,
}

impl<'a> Unpin for Notified<'a> {}

impl<'a> Future for Notified<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = Pin::new(self);

        loop {
            if this.receiver.get_notified() {
                return Poll::Ready(());
            }

            match &mut this.listener {
                None => {
                    this.listener = Some(this.receiver.channel.notify_event.listen());
                }
                Some(l) => match Pin::new(l).poll(cx) {
                    Poll::Ready(_) => {
                        this.listener = None;
                        continue;
                    }

                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
