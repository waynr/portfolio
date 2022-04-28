use axum::{routing::delete, Router};

use crate::http::notimplemented;

pub fn router() -> Router {
    Router::new().route(
        "/:reference",
        delete(notimplemented)
            .put(notimplemented)
            .get(notimplemented)
            .head(notimplemented),
    )
}
