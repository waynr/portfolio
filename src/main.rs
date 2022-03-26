use std::collections::HashMap;

use axum::{
    extract::Path,
    routing::{delete, get, post, patch},
    Router,
};


#[tokio::main]
async fn main() {
    let blobs = Router::new().route( "/:digest", get(notimplemented).delete(notimplemented).head(notimplemented))
        .route("/uploads/", post(notimplemented))
        .route("/uploads/:reference", patch(notimplemented).put(notimplemented));
    let manifests = Router::new().route("/:reference", delete(notimplemented).put(notimplemented).get(notimplemented).head(notimplemented));

    let tags = Router::new().route("/list", get(notimplemented));

    let app = Router::new().route("/:registry/v2", get(hello_world))
        .nest("/:registry/v2/:repository/blobs", blobs)
        .nest("/:registry/v2/:repository/manifests", manifests)
        .nest("/:registry/v2/:repository/tags", tags);

    axum::Server::bind(&"0.0.0.0:13030".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}


async fn notimplemented(Path(params): Path<HashMap<String, String>>) -> String { 
    format!("not implemented\n{:?}", params)
}

async fn hello_world() -> String {
    "hello world".to_string()
}
