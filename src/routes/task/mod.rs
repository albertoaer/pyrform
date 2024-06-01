use axum::{routing::{get, post}, Router};

mod handlers;
mod state;

pub fn router(state: impl Into<state::TaskState>) -> Router {
  Router::new()
    .route("/", get(handlers::list_tasks).post(handlers::queue_task))
    .route("/:id", get(handlers::get_task))
    .route("/:id/cancel", post(handlers::cancel_task))
    .with_state(state.into())
}