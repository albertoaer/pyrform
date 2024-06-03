use axum::Router;
use clap::Parser;
use services::worker::WorkerService;
use tower_http::cors::CorsLayer;
use tracing::{info, Level};

mod model;
mod routes;
mod services;
mod args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let args = args::Args::parse();

  let subscriber = tracing_subscriber::fmt::fmt().with_max_level(Level::TRACE).finish();
  tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

  let cors = CorsLayer::new().allow_origin(tower_http::cors::Any).allow_methods(tower_http::cors::Any);

  let worker_service = WorkerService::new("./workers/")?;

  let mut app = Router::new()
    .nest("/task", routes::task::router(&worker_service));
  if args.edit_workers {
    app = app.nest("/worker", routes::worker::router(&worker_service));
  }
  app = app.layer(cors);

  let listener = tokio::net::TcpListener::bind(args.bind).await.unwrap();

  info!("server started");

  Ok(axum::serve(listener, app).await.unwrap())
}
