use std::{collections::HashMap, time::Duration};

use axum::{extract::{Path, Query, State}, http::StatusCode, response::IntoResponse, Json};
use serde::{Serialize, Deserialize};
use serde_json::json;
use serde_with::{serde_as, DurationSeconds};
use tokio::{select, time};
use uuid::Uuid;

use crate::model::Task;

use super::state::{TaskManager, TaskState};

pub async fn list_tasks(State(TaskState { queued, .. }): State<TaskState>) -> impl IntoResponse {
  let data = HashMap::<_, _>::from_iter(
    queued.lock().await.iter().map(|(name, manager)| (name.to_string(), manager.status.borrow().clone()))
  );

  (StatusCode::OK, Json(json!(data)))
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct QueueTaskQuery {
  #[serde(default)]
  r#await: bool,
  #[serde(default)]
  #[serde_as(as = "DurationSeconds<f64>")]
  timeout: Duration
}

pub async fn queue_task(
  State(TaskState { worker_service, queued }): State<TaskState>,
  Query(QueueTaskQuery { r#await, timeout }): Query<QueueTaskQuery>,
  Json(task): Json<Task>,
) -> impl IntoResponse {
  let (stop, status) = match worker_service.queue_task(task.clone()).await {
    Ok(channels) => channels,
    Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": err.to_string() }))),
  };

  let uuid = Uuid::new_v4();
  let id = uuid.to_string();

  let mut info = status.clone();
  queued.lock().await.insert(uuid, TaskManager {
    task,
    status,
    stop: Some(stop)
  });

  if !r#await {
    return (StatusCode::OK, Json(json!({ "id": id, "info": info.borrow().clone() })))
  }

  println!("{:?}", timeout);
  const DEFAULT_AWAIT_TIMEOUT: Duration = Duration::from_secs(10);
  let timeout = time::sleep(if timeout.is_zero() { DEFAULT_AWAIT_TIMEOUT } else { timeout });
  tokio::pin!(timeout);

  loop {
    select! {
      changed = info.changed() => {
        if let Err(err) = changed {
          return (StatusCode::OK, Json(json!({ "error": err.to_string() })))
        }

        let current_info = info.borrow();
        if current_info.is_finished() {
          return (StatusCode::OK, Json(json!({ "id": id, "info": current_info.clone() })))
        }
      }
      _ = &mut timeout => return (StatusCode::OK, Json(json!({ "id": id, "info": info.borrow().clone() })))
    }
  }
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