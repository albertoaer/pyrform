use axum::{routing::get, Router};

mod handlers;
mod state;

pub fn router(state: impl Into<state::WorkerState>) -> Router {
  Router::new()
    .route("/:name", 
      get(handlers::get_worker)
      .post(handlers::add_worker)
      .delete(handlers::del_worker)
    )
    .with_state(state.into())
}