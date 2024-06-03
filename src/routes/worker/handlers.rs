use axum::{extract::{Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}};
use serde::{Deserialize, Serialize};

use crate::model::CreateWorkerData;

use super::state::WorkerState;

pub async fn get_worker(
  State(WorkerState { worker_service }): State<WorkerState>,
  Path(name): Path<String>
) -> Response {
  match worker_service.get_worker(name).await {
    Ok(result) => result.into_response(),
    Err(err) => (StatusCode::NOT_FOUND, err.to_string()).into_response()
  }
}

#[derive(Serialize, Deserialize)]
pub struct ReplaceQuery {
  #[serde(default)]
  replace: bool
}

pub async fn add_worker(
  State(WorkerState { worker_service }): State<WorkerState>,
  Path(name): Path<String>,
  Query(replace): Query<ReplaceQuery>,
  source: String
) -> Response {
  match worker_service.add_worker(CreateWorkerData {
    name,
    source,
    replace: replace.replace,
}).await {
    Ok(result) => result.into_response(),
    Err(err) => (StatusCode::CONFLICT, err.to_string()).into_response()
  }
}

pub async fn del_worker(
  State(WorkerState { worker_service }): State<WorkerState>,
  Path(name): Path<String>
) -> Response {
  match worker_service.del_worker(name).await {
    Ok(result) => result.into_response(),
    Err(err) => (StatusCode::NOT_FOUND, err.to_string()).into_response()
  }
}