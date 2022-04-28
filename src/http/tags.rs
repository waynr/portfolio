use axum::{
    routing::get,
    Router,
};

use crate::http::notimplemented;

pub fn router() -> Router {
    Router::new().route("/list", get(notimplemented))
}
