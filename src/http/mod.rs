use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::{self as axum_middleware, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tower_http::trace::{self, TraceLayer};

pub mod headers;

pub(crate) mod blobs;
mod manifests;
mod tags;

use crate::errors::Result;
use crate::objects::ObjectStore;
use crate::Portfolio;

async fn auth<B, O: ObjectStore>(
    State(portfolio): State<Portfolio<O>>,
    mut req: Request<B>,
    next: Next<B>,
) -> std::result::Result<Response, StatusCode> {
    // TODO: implement actual authentication
    let registry = match portfolio.get_registry("meow").await {
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
        Ok(r) => r,
    };
    req.extensions_mut().insert(registry);

    Ok(next.run(req).await)
}

//pub async fn serve<O: ObjectStore>(metadata: PostgresMetadata, objects: O) -> Result<()> {
pub async fn serve<O: ObjectStore>(portfolio: Portfolio<O>) -> Result<()> {
    let blobs = blobs::router::<O>();
    let manifests = manifests::router::<O>();
    let tags = tags::router::<O>();

    let app = Router::new()
        .route("/v2", get(hello_world))
        .nest("/v2/:repository/blobs", blobs)
        .nest("/v2/:repository/manifests", manifests)
        .nest("/v2/:repository/tags", tags)
        .layer(
            TraceLayer::new_for_http()
                .on_response(trace::DefaultOnResponse::new())
                .on_request(trace::DefaultOnRequest::new()),
        )
        .route_layer(axum_middleware::from_fn_with_state(portfolio.clone(), auth));

    axum::Server::bind(&"0.0.0.0:13030".parse()?)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn hello_world() -> Result<Response> {
    Ok((StatusCode::OK, "").into_response())
}
