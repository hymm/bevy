use core::fmt;
use futures_lite::Future;
use pin_project_lite::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Polls a future just once and returns an [`Option`] with the result.
pub fn poll_ten<T, F>(f: F) -> PollTen<F>
where
    F: Future<Output = T>,
{
    PollTen { f, count: 0 }
}

pin_project! {
    /// Future for the [`poll_ten()`] function.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct PollTen<F> {
        #[pin]
        f: F,
        #[pin]
        count: usize
    }
}

impl<F> fmt::Debug for PollTen<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollOnce").finish()
    }
}

impl<T, F> Future for PollTen<F>
where
    F: Future<Output = T>,
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.f.poll(cx) {
            Poll::Ready(t) => Poll::Ready(Some(t)),
            Poll::Pending => {
                if *this.count < 10 {
                    *this.count += 1;
                    Poll::Pending
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}
