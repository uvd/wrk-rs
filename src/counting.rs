// Wraps an AsyncRead+AsyncWrite stream to count the number of bytes read,
// so the worker can tally bytes received exactly like wrk (which adds every
// successful read's byte count to thread->bytes, including response headers).

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct CountingStream<S> {
    inner: S,
    bytes_read: Arc<AtomicU64>,
}

impl<S> CountingStream<S> {
    pub fn new(inner: S, bytes_read: Arc<AtomicU64>) -> Self {
        CountingStream { inner, bytes_read }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Safety: we never move `inner` out; we project through to it.
        let this = unsafe { self.get_unchecked_mut() };
        let before = buf.filled().len();
        let pinned = unsafe { Pin::new_unchecked(&mut this.inner) };
        match pinned.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                if after > before {
                    this.bytes_read
                        .fetch_add((after - before) as u64, Ordering::Relaxed);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };
        let pinned = unsafe { Pin::new_unchecked(&mut this.inner) };
        pinned.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = unsafe { self.get_unchecked_mut() };
        let pinned = unsafe { Pin::new_unchecked(&mut this.inner) };
        pinned.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = unsafe { self.get_unchecked_mut() };
        let pinned = unsafe { Pin::new_unchecked(&mut this.inner) };
        pinned.poll_shutdown(cx)
    }
}
