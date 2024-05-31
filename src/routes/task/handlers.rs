use axum::{extract::State, response::IntoResponse, Json};

use super::state::TaskState;

pub async fn list_tasks(State(TaskState { worker_service }): State<TaskState>) -> impl IntoResponse {
  Json::from(serde_json::json!([]))
}

pub async fn publish_task(State(TaskState { worker_service }): State<TaskState>) -> impl IntoResponse {
  Json::from(serde_json::json!({}))
}