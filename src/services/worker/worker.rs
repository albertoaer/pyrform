use std::sync::Arc;

use tokio::{sync::{mpsc, Mutex, RwLock}, time::Instant};

use super::{python::start_python_loop, task::TaskRunInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRunInfo {
  pub source: String,
  pub worker_path: String,
  pub worker_name: String,
  pub instant: Instant
}

pub type SharedWorkerInfo = Arc<RwLock<Option<WorkerRunInfo>>>;

#[derive(Clone)]
pub struct Worker {
  run_info: SharedWorkerInfo,
  tasks: (mpsc::Sender<TaskRunInfo>, Arc<Mutex<mpsc::Receiver<TaskRunInfo>>>)
}

const WORKER_TASKS_CAPACITY: usize = 10;

impl Worker {
  pub fn new(run_info: SharedWorkerInfo) -> Self {
    let (sender, receiver) = mpsc::channel(WORKER_TASKS_CAPACITY);
    Self {
      run_info,
      tasks: (sender, Arc::new(Mutex::new(receiver))),
    }
  }

  pub async fn queue_task(&self, task: TaskRunInfo) {
    let _ = self.tasks.0.send(task).await;
  }

  pub async fn worker_loop(self) {
    let tasks = self.tasks.clone();

    let _ = start_python_loop(move || {
      let run_info = self.run_info.clone();
      let (task_sender, task_receiver) = tasks.clone();

      async move {
        let task = loop {
          let task = task_receiver.lock().await.recv().await.expect("closed channel");
          if !task.stopped().await {
            break task
          }
        };
        let run_info = {
          match run_info.read().await.clone() {
            Some(run_info) => run_info,
            None => {
              task_sender.send(task).await.expect("channel closed"); // prevent leaked tasks
              return None
            }
          }
        };
        Some((run_info, task))
      }
    }).await;
  }
}