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
    finished: Arc<AtomicBool>,
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
        self.channel.finished.store(true, Ordering::Relaxed);
        self.channel.finish_event.notify(usize::MAX);
    }

    pub fn reset(&mut self) {
        self.channel.finished.store(false, Ordering::Relaxed);
    }
}


#[derive(Clone)]
pub struct Receiver {
    channel: Arc<Channel>,
}

impl Receiver {
    fn get_finished(&self) -> bool {
        self.channel.finished.load(Ordering::Relaxed)
    }

    pub fn finished(&self) -> Finished<'_> {
        Finished {
            receiver: self,
            listener: None,
        }
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
            if this.receiver.get_finished() {
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
