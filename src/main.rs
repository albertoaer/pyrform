use axum::Router;
use clap::Parser;
use services::worker::WorkerService;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
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

  let worker_service = WorkerService::new(&args.path)?;

  let mut app = Router::new()
    .nest("/task", routes::task::router(&worker_service));
  if args.edit_workers {
    app = app.nest("/worker", routes::worker::router(&worker_service));
  }
  app = app.layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()).layer(cors));

  let listener = tokio::net::TcpListener::bind(&args.bind).await.unwrap();

  info!("workers path: {}", args.path);
  info!("binded: {}", args.bind);
  info!("server started");

  Ok(axum::serve(listener, app).await.unwrap())
}
