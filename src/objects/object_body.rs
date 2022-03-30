use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::stream::Stream;
use hyper::body::Body;

use crate::Digester;

type StreamableBody = Box<
    (dyn futures_core::stream::Stream<
        Item = std::result::Result<
            axum::body::Bytes,
            Box<dyn std::error::Error + Send + Sync + 'static>,
        >,
    > + Send),
>;

impl StreamObjectBody {
    pub fn from_body(body: Body, digester: Digester) -> StreamableBody {
        Box::new(StreamObjectBody {
            body: ObjectBody { body, digester },
        })
    }
}

pub struct ObjectBody {
    body: Body,
    digester: Digester,
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

    // fn pin_get_body(self: Pin<&mut Self>) -> Pin<&mut ObjectBody> {
    //     // this is "safe" because body is never considered pin, see
    //     // https://doc.rust-lang.org/std/pin/#pinning-is-structural-for-field
    //     unsafe { self.map_unchecked_mut(|s| &mut s.body) }
    // }

    // fn pin_get_digester(&mut self) -> &mut T {
    //     unsafe { &mut self.get_unchecked_mut().digester }
    // }
}

impl Stream for StreamObjectBody {
    type Item =
        std::result::Result<axum::body::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let object_body = self.pin_get_body();
        match Pin::new(&mut object_body.body).poll_next(cx) {
            Poll::Ready(opt) => {
                match opt {
                    Some(result) => {
                        match result {
                            Ok(bytes) => {
                                // and this is where we calculate incremental digest
                                object_body.digester.update(bytes.as_ref());
                                Poll::Ready(Some(Ok(bytes)))
                            }
                            Err(e) => Poll::Ready(Some(Err(Box::new(e)))),
                        }
                    }
                    None => Poll::Ready(None),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
