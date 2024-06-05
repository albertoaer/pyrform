use axum::{extract::{Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::model::CreateWorkerData;

use super::state::WorkerState;

pub async fn get_worker(
  State(WorkerState { worker_service }): State<WorkerState>,
  Path(name): Path<String>
) -> impl IntoResponse {
  match worker_service.get_worker(name).await {
    Ok(source) => (StatusCode::OK, source.into_response()),
    Err(err) => (StatusCode::NOT_FOUND, Json(json!({ "error": err.to_string() })).into_response())
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
  match worker_service.add_worker(CreateWorkerData { name, source, replace: replace.replace, }).await {
    Ok(_) => StatusCode::OK.into_response(),
    Err(err) => (StatusCode::CONFLICT, Json(json!({ "error": err.to_string() }))).into_response()
  }
}

pub async fn del_worker(
  State(WorkerState { worker_service }): State<WorkerState>,
  Path(name): Path<String>
) -> Response {
  match worker_service.del_worker(name).await {
    Ok(_) => StatusCode::OK.into_response(),
    Err(err) => (StatusCode::NOT_FOUND, Json(json!({ "error": err.to_string() }))).into_response()
  }
}