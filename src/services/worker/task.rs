use std::sync::Arc;

use pyo3::{pyclass, pymethods};
use tokio::sync::{oneshot, watch, Mutex};

use crate::model::{self, TaskStatus};

#[derive(Clone)]
#[pyclass]
pub struct Task {
  worker: String,
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
      title: task.title,
      dedicated: task.dedicated,
      args: task.args,
    }
  }
}

#[derive(Clone)]
pub struct TaskRunInfo {
  pub task: Task,
  pub stop: Arc<Mutex<oneshot::Receiver<()>>>,
  pub status: watch::Sender<TaskStatus>
}