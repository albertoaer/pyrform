use std::{collections::HashMap, sync::Arc};

use tokio::sync::{oneshot, watch, Mutex};
use uuid::Uuid;

use crate::{model::{Task, TaskStatus}, services::worker::WorkerService};

pub struct TaskManager {
  pub task: Task,
  pub status: watch::Receiver<TaskStatus>,
  pub stop: Option<oneshot::Sender<()>>
}

#[derive(Clone)]
pub struct TaskState {
  pub worker_service: WorkerService,
  pub queued: Arc<Mutex<HashMap<Uuid, TaskManager>>>
}

impl From<&WorkerService> for TaskState {
  fn from(worker_service: &WorkerService) -> Self {
    Self {
      worker_service: worker_service.clone(),
      queued: Arc::new(Mutex::new(HashMap::new()))
    }
  }
}