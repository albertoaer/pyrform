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
  args: Vec<String>,
  delay: Duration,
  queue_again: bool
}

#[pymethods]
impl Task {
  pub fn __repr__(&self) -> String {
    format!("TASK['{}']", self.title.clone().unwrap_or("no title".to_string()))
  }

  #[getter]
  pub fn worker(&self) -> String {
    self.worker.clone()
  }

  #[setter]
  pub fn set_worker(&mut self, worker: String) {
    self.worker = worker
  }

  #[getter]
  pub fn function(&self) -> String {
    self.function.clone()
  }

  #[setter]
  pub fn set_function(&mut self, function: String) {
    self.function = function
  }

  #[getter]
  pub fn title(&self) -> Option<String> {
    self.title.clone()
  }

  #[setter]
  pub fn set_title(&mut self, title: Option<String>) {
    self.title = title
  }

  #[getter]
  pub fn dedicated(&self) -> bool {
    self.dedicated
  }

  #[setter]
  pub fn set_dedicated(&mut self, title: Option<String>) {
    self.title = title
  }

  #[getter]
  pub fn args(&self) -> Vec<String> {
    self.args.clone()
  }

  #[setter]
  pub fn set_args(&mut self, args: Vec<String>) {
    self.args = args
  }

  #[getter]
  pub fn delay(&self) -> Duration {
    self.delay.clone()
  }

  #[setter]
  pub fn set_delay(&mut self, delay: Duration) {
    self.delay = delay
  }

  #[getter]
  pub fn is_queue_again(&self) -> bool {
    self.queue_again
  }

  pub fn queue_again(&mut self, prevent: Option<bool>) {
    self.queue_again = prevent.unwrap_or(true)
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
      delay: task.delay,
      queue_again: false
    }
  }
}

impl From<Task> for model::Task {
  fn from(task: Task) -> Self {
    Self {
      worker: task.worker,
      function: task.function,
      title: task.title,
      dedicated: task.dedicated,
      delay: task.delay,
      args: task.args
    }
  }
}

#[derive(Debug, Clone)]
pub struct TaskRunInfo {
  pub task: Arc<model::Task>,
  pub stop: Arc<Mutex<oneshot::Receiver<()>>>,
  pub status: watch::Sender<TaskStatus>,
  pub creation: Instant,
  pub execution: Instant,
}

impl TaskRunInfo {
  pub fn new(task: model::Task, stop: oneshot::Receiver<()>, status: watch::Sender<TaskStatus>) -> Self {
    Self {
      task: Arc::new(task),
      stop: Arc::new(Mutex::new(stop)),
      status,
      creation: Instant::now(),
      execution: Instant::now()
    }
  }

  pub fn replace_task(mut self, task: model::Task) -> Self {
    self.task = Arc::new(task);
    self
  }

  pub fn set_execution_now(&mut self) {
    self.execution = Instant::now()
  }

  pub fn task_for_python(&self) -> Task {
    (*self.task).clone().into()
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