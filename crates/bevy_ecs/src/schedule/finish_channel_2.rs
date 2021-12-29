use event_listener::{Event, EventListener};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

#[derive(Default)]
struct Channel {
    dependencies_total: Arc<AtomicUsize>,
    dependencies_finished_count: Arc<AtomicUsize>,
    finish_event: Event,
}

pub fn finish_channel() -> (Sender, Receiver) {
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
    pub fn finish(&mut self) {
        self.channel
            .dependencies_finished_count
            .fetch_add(1, Ordering::Relaxed);
        self.channel.finish_event.notify(usize::MAX);
    }
}

#[derive(Clone)]
pub struct Receiver {
    channel: Arc<Channel>,
}

impl Receiver {
    pub fn finished(&self) -> Finished<'_> {
        Finished {
            receiver: self,
            listener: None,
        }
    }

    fn is_finished(&self) -> bool {
        self.channel
            .dependencies_finished_count
            .load(Ordering::Relaxed)
            >= self.channel.dependencies_total.load(Ordering::Relaxed)
    }

    pub fn set_dependencies_total(&mut self, total: usize) {
        self.channel
            .dependencies_total
            .store(total, Ordering::Relaxed);
    }

    pub fn reset(&mut self) {
        self.channel
            .dependencies_finished_count
            .store(0, Ordering::Relaxed);
    }
}

pub struct Finished<'a> {
    receiver: &'a Receiver,
    listener: Option<EventListener>,
}

impl<'a> Unpin for Finished<'a> {}

impl<'a> Future for Finished<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = Pin::new(self);

        loop {
            if this.receiver.is_finished() {
                return Poll::Ready(());
            }

            match &mut this.listener {
                None => {
                    this.listener = Some(this.receiver.channel.finish_event.listen());
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
