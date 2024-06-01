use std::time::Duration;

use tokio::{select, sync::{oneshot, watch}, time::sleep};

use crate::model::{Task, TaskStatus};

#[derive(Clone)]
pub struct WorkerService;

impl WorkerService {
  pub fn new() -> Self {
    WorkerService
  }
}

impl WorkerService {
  pub async fn queue_task(&mut self, task: Task, stop: oneshot::Receiver<()>) -> watch::Receiver<TaskStatus> {
    let (tx, rx) = watch::channel(TaskStatus::Pending);

    tokio::spawn(async move {
      sleep(Duration::from_secs(10)).await;
      tx.send(TaskStatus::Running).ok();
      let result = select! {
        _ = sleep(Duration::from_secs(10)) => TaskStatus::Done,
        _ = stop => TaskStatus::Cancelled
      };
      tx.send(result).ok();
    });

    rx
  }
}