use std::{sync::Arc, time::Duration};

use pyo3::{pyclass, pymethods};
use tokio::{sync::{oneshot, watch, Mutex}, time::Instant};

use crate::model::{self, TaskStatus};

#[derive(Clone)]
#[pyclass]
pub struct Task {
  worker: String,
  function: String,
  title: Option<String>,
  dedicated: bool,
  args: Vec<String>
}

#[pymethods]
impl Task {
  fn __repr__(&self) -> String {
    format!("TASK['{}']", self.title.clone().unwrap_or("no title".to_string()))
  }

  #[getter]
  fn worker(&self) -> String {
    self.worker.clone()
  }

  #[getter]
  fn function(&self) -> String {
    self.function.clone()
  }

  #[getter]
  fn title(&self) -> Option<String> {
    self.title.clone()
  }

  #[getter]
  fn dedicated(&self) -> bool {
    self.dedicated
  }

  #[getter]
  fn args(&self) -> Vec<String> {
    self.args.clone()
  }
}

impl From<model::Task> for Task {
  fn from(task: model::Task) -> Self {
    Self {
      worker: task.worker,
      function: task.function,
      title: task.title,
      dedicated: task.dedicated,
      args: task.args,
    }
  }
}

#[derive(Clone)]
pub struct TaskRunInfo {
  pub task: model::Task,
  pub stop: Arc<Mutex<oneshot::Receiver<()>>>,
  pub status: watch::Sender<TaskStatus>,
  pub creation: Instant,
  pub execution: Instant,
}

impl TaskRunInfo {
  pub fn new(task: model::Task, stop: oneshot::Receiver<()>, status: watch::Sender<TaskStatus>) -> Self {
    Self {
      task,
      stop: Arc::new(Mutex::new(stop)),
      status,
      creation: Instant::now(),
      execution: Instant::now()
    }
  }

  pub fn set_execution_now(&mut self) {
    self.execution = Instant::now()
  }

  pub fn task_for_python(&self) -> Task {
    self.task.clone().into()
  }

  pub async fn stopped(&self) -> bool {
    self.stop.lock().await.try_recv().ok().is_some()
  }

  pub fn set_status(&self, status: TaskStatus) {
    let _ = self.status.send(status);
  }

  /// returns (`total`, `execution`)
  pub fn get_duration_now(&self) -> (Duration, Duration) {
    let now = Instant::now();
    (now - self.creation, now - self.execution)
  }
}