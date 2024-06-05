use std::collections::HashMap;

use axum::{extract::{Path, State}, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use uuid::Uuid;

use crate::model::Task;

use super::state::{TaskManager, TaskState};

pub async fn list_tasks(State(TaskState { queued, .. }): State<TaskState>) -> impl IntoResponse {
  let data = HashMap::<_, _>::from_iter(
    queued.lock().await.iter().map(|(name, manager)| (name.to_string(), manager.status.borrow().clone()))
  );

  (StatusCode::OK, Json(json!(data)))
}

pub async fn queue_task(
  State(TaskState { worker_service, queued }): State<TaskState>,
  Json(task): Json<Task>
) -> impl IntoResponse {
  let (stop, status) = match worker_service.queue_task(task.clone()).await {
    Ok(channels) => channels,
    Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": err.to_string() }))),
  };

  let uuid = Uuid::new_v4();
  let id = uuid.to_string();

  let info = status.borrow().clone();
  queued.lock().await.insert(uuid, TaskManager {
    task,
    status,
    stop: Some(stop)
  });

  (StatusCode::OK, Json(json!({ "id": id, "info": info })))
}

pub async fn get_task(
  State(TaskState { queued, .. }): State<TaskState>,
  Path(id): Path<String>
) -> impl IntoResponse {
  let status = queued.lock().await.iter()
    .find(|(task_id, _)| task_id.to_string() == id)
    .and_then(|(_, manager)| Some(manager.status.borrow().clone()));

  match status {
    Some(status) => (StatusCode::OK, Json(json!({ "id": id, "info": status }))),
    None => (StatusCode::NOT_FOUND, Json(json!({ "error": format!("{} not found", id) }))),
  }
}

pub async fn cancel_task(
  State(TaskState { queued, .. }): State<TaskState>,
  Path(id): Path<String>
) -> impl IntoResponse {
  let mut lock = queued.lock().await;
  let stop = lock.iter_mut()
    .find(|(task_id, _)| task_id.to_string() == id)
    .and_then(|(_, manager)| manager.stop.take().and_then(|stop| Some((manager.status.borrow().clone(), stop))));

  match stop {
    Some((status, stop)) if status.can_be_cancelled() => {
      let _ = stop.send(());

      (StatusCode::OK, Json(json!({ "result": "accepted" })))
    },
    _ => (StatusCode::NOT_FOUND, Json(json!({ "error": format!("{} not found or cannot be cancelled", id)}))),
  }
}