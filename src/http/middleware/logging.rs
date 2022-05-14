use std::fmt;
use std::task::{Context, Poll};

use tower::{Layer, Service};

pub struct LogLayer {
    pub target: &'static str,
}

impl<S> Layer<S> for LogLayer {
    type Service = Logerator<S>;

    fn layer(&self, service: S) -> Self::Service {
        Logerator {
            target: self.target,
            service,
        }
    }
}

#[derive(Clone)]
pub struct Logerator<S> {
    target: &'static str,
    service: S,
}

impl<S, R> Service<R> for Logerator<S>
where
    S: Service<R>,
    R: fmt::Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        // Insert log statement here or other functionality
        println!("request = {:?}, target = {:?}", request, self.target);
        self.service.call(request)
    }
}
