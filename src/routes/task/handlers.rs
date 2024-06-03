use std::collections::HashMap;

use axum::{extract::{Path, State}, http::StatusCode, response::{IntoResponse, Response}, Json};
use uuid::Uuid;

use crate::model::Task;

use super::state::{TaskManager, TaskState};

pub async fn list_tasks(State(TaskState { queued, .. }): State<TaskState>) -> impl IntoResponse {
  let data = HashMap::<_, _>::from_iter(
    queued.lock().await.iter().map(|(name, manager)| (name.to_string(), manager.status.borrow().clone()))
  );

  Json::from(serde_json::json!(data))
}

pub async fn queue_task(
  State(TaskState { mut worker_service, queued }): State<TaskState>,
  Json(task): Json<Task>
) -> impl IntoResponse {
  let (stop, status) = match worker_service.queue_task(task.clone()).await {
    Ok(channels) => channels,
    Err(err) => return Json(serde_json::json!({
      "error": err.to_string()
    })),
  };

  let uuid = Uuid::new_v4();
  let id = uuid.to_string();

  let info = status.borrow().clone();
  queued.lock().await.insert(uuid, TaskManager {
    task,
    status,
    stop: Some(stop)
  });

  Json(serde_json::json!({
    "id": id,
    "info": info
  }))
}

pub async fn get_task(
  State(TaskState { queued, .. }): State<TaskState>,
  Path(id): Path<String>
) -> Response {
  let status = queued.lock().await.iter()
    .find(|(task_id, _)| task_id.to_string() == id)
    .and_then(|(_, manager)| Some(manager.status.borrow().clone()));

  match status {
    Some(status) => Json(serde_json::json!({
      "id": id,
      "info": status
    })).into_response(),
    None => (StatusCode::NOT_FOUND, format!("{} not found", id)).into_response(),
  }
}

pub async fn cancel_task(
  State(TaskState { queued, .. }): State<TaskState>,
  Path(id): Path<String>
) -> Response {
  let mut lock = queued.lock().await;
  let stop = lock.iter_mut()
    .find(|(task_id, _)| task_id.to_string() == id)
    .and_then(|(_, manager)| manager.stop.take().and_then(|stop| Some((manager.status.borrow().clone(), stop))));

  match stop {
    Some((status, stop)) if status.can_be_cancelled() => {
      let _ = stop.send(());

      Json(serde_json::json!({
        "result": "accepted"
      })).into_response()
    },
    _ => (StatusCode::NOT_FOUND, format!("{} not found or cannot be cancelled", id)).into_response(),
  }
}