use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures_core::stream::Stream;
use hyper::body::Body;
use pin_project::pin_project;

use crate::Digester;

type StreamableBody = Box<
    (dyn futures_core::stream::Stream<
        Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Send),
>;

/// Wrapper around [`hyper::body::Body`] that calculates the digest of the contents as they are
/// read.
///
/// Makes use of [`super::Digester`] to incrementally calculate the digest of the stream bytes as
/// they are read and forwarded on to the next consumer.
#[pin_project]
pub struct DigestBody {
    body: Body,
    digester: Arc<Mutex<Digester>>,
}

impl DigestBody {
    pub fn from_body(body: Body, digester: Arc<Mutex<Digester>>) -> StreamableBody {
        Box::new(Self { body, digester })
    }
}

impl Stream for DigestBody {
    type Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match Pin::new(&mut this.body).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                // and this is where we calculate incremental digest
                {
                    let mut g = this.digester.lock().expect(
                        "only one instance of the digester should ever be active at a time",
                    );
                    g.update(bytes.as_ref());
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

const CHUNK_SIZE: usize = 6 * 1024 * 1024; // 6 MB

/// Turn a [`hyper::body::Body`] into a stream of fixed-size [`bytes::Bytes`].
///
/// Wrapper around [`hyper::body::Body`] that buffers and re-streams the underlying stream bytes
/// into fixed size chunks of bytes.
#[pin_project]
pub struct ChunkedBody {
    body: Body,
    buffer: BytesMut,
}

impl ChunkedBody {
    pub fn from_body(body: Body) -> ChunkedBody {
        ChunkedBody {
            body,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
        }
    }
}

impl Stream for ChunkedBody {
    type Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match Pin::new(this.body).poll_next(cx) {
            Poll::Ready(Some(Ok(mut bytes))) => {
                let remaining = CHUNK_SIZE - this.buffer.len();
                if bytes.len() < remaining {
                    this.buffer.extend_from_slice(&bytes);
                    return Poll::Pending;
                }
                if bytes.len() == remaining {
                    this.buffer.extend_from_slice(&bytes);
                    let buf = this.buffer.split();
                    return Poll::Ready(Some(Ok(buf.freeze())));
                } else {
                    let exact = bytes.split_to(remaining);
                    this.buffer.extend_from_slice(&exact);
                    let buf = this.buffer.split();
                    this.buffer.extend_from_slice(&bytes);
                    return Poll::Ready(Some(Ok(buf.freeze())));
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => {
                if this.buffer.len() > 0 {
                    let buf = this.buffer.split();
                    return Poll::Ready(Some(Ok(buf.freeze())));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
