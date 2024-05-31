use axum::{routing::get, Router};

mod handlers;
mod state;

pub fn router(state: impl Into<state::TaskState>) -> Router {
  Router::new()
    .route("/", get(handlers::list_tasks).post(handlers::publish_task))
    .with_state(state.into())
}