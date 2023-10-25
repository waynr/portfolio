use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures_core::stream::Stream;
use hyper::body::Body;

use crate::Digester;

type StreamableBody = Box<
    (dyn futures_core::stream::Stream<
        Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Send),
>;

impl StreamObjectBody {
    pub fn from_body(body: Body, digester: Arc<Mutex<Digester>>) -> StreamableBody {
        Box::new(StreamObjectBody {
            body: ObjectBody { body, digester },
        })
    }
}

pub struct ObjectBody {
    body: Body,
    digester: Arc<Mutex<Digester>>,
}

pub struct StreamObjectBody {
    body: ObjectBody,
}

impl StreamObjectBody {
    fn pin_get_body(self: Pin<&mut Self>) -> &mut ObjectBody {
        // this is "safe" because body is never considered pin, see
        // https://doc.rust-lang.org/std/pin/#pinning-is-not-structural-for-field
        unsafe { &mut self.get_unchecked_mut().body }
    }
}

impl Stream for StreamObjectBody {
    type Item = std::result::Result<Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let object_body = self.pin_get_body();
        match Pin::new(&mut object_body.body).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                // and this is where we calculate incremental digest
                {
                    let mut g = object_body.digester.lock().expect(
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

pub struct ChunkedBody {
    body: Body,
    buffer: BytesMut,
}

impl ChunkedBody {
    fn pin_get_body(self: Pin<&mut Self>) -> &mut Body {
        // this is "safe" because body is never considered pin, see
        // https://doc.rust-lang.org/std/pin/#pinning-is-not-structural-for-field
        unsafe { &mut self.get_unchecked_mut().body }
    }
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let body = self.as_mut().pin_get_body();
        match Pin::new(body).poll_next(cx) {
            Poll::Ready(Some(Ok(mut bytes))) => {
                let remaining = CHUNK_SIZE - self.buffer.len();
                if bytes.len() < remaining {
                    self.buffer.extend_from_slice(&bytes);
                    return Poll::Pending;
                }
                if bytes.len() == remaining {
                    self.buffer.extend_from_slice(&bytes);
                    let buf = self.buffer.split();
                    return Poll::Ready(Some(Ok(buf.freeze())));
                } else {
                    let exact = bytes.split_to(remaining);
                    self.buffer.extend_from_slice(&exact);
                    let buf = self.buffer.split();
                    self.buffer.extend_from_slice(&bytes);
                    return Poll::Ready(Some(Ok(buf.freeze())));
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => {
                if self.buffer.len() > 0 {
                    let buf = self.buffer.split();
                    return Poll::Ready(Some(Ok(buf.freeze())));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
