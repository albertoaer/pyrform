use axum::Router;
use services::worker::WorkerService;
use tower_http::cors::CorsLayer;

mod model;
mod routes;
mod services;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let cors = CorsLayer::new().allow_origin(tower_http::cors::Any).allow_methods(tower_http::cors::Any);

  let worker_service = WorkerService::new("./workers/")?;

  let app = Router::new()
    .nest("/task", routes::task::router(&worker_service))
    .layer(cors);

  let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
  Ok(axum::serve(listener, app).await.unwrap())
}
