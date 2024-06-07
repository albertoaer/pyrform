use std::sync::{atomic::{AtomicIsize, AtomicUsize, Ordering}, Arc};

use tokio::{sync::{mpsc, Mutex, RwLock}, time::Instant};
use tracing::{debug, error};

use crate::model::TaskStatus;

use super::task::TaskRunInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRunInfo {
  pub source: String,
  pub path: String,
  pub name: String,
  pub instant: Instant
}

pub type SharedWorkerInfo = Arc<RwLock<Option<WorkerRunInfo>>>;

pub struct WorkerTaskError {
  pub other_error: anyhow::Error,
  pub run_info: Option<WorkerRunInfo>,
  pub task_info: Option<TaskRunInfo>,
  pub fail: bool // fail means that the error what expecific during task execution time
}

pub type WorkerTaskResult<T> = Result<T, WorkerTaskError>;

pub trait IntoWorkerTaskResult<T> {
  fn worker_task_fail(self, run_info: Option<&WorkerRunInfo>, task_info: Option<&TaskRunInfo>) -> WorkerTaskResult<T>;
  fn worker_task_error(self, run_info: Option<&WorkerRunInfo>, task_info: Option<&TaskRunInfo>) -> WorkerTaskResult<T>;
}

impl<T, E> IntoWorkerTaskResult<T> for Result<T, E> where E : Into<anyhow::Error> {
  fn worker_task_fail(self, run_info: Option<&WorkerRunInfo>, task_info: Option<&TaskRunInfo>) -> WorkerTaskResult<T> {
    self.map_err(|err| WorkerTaskError {
      other_error: err.into(),
      run_info: run_info.map(|x| x.clone()),
      task_info: task_info.map(|x| x.clone()),
      fail: true
    })
  }

  fn worker_task_error(self, run_info: Option<&WorkerRunInfo>, task_info: Option<&TaskRunInfo>) -> WorkerTaskResult<T> {
    self.map_err(|err| WorkerTaskError {
      other_error: err.into(),
      run_info: run_info.map(|x| x.clone()),
      task_info: task_info.map(|x| x.clone()),
      fail: false
    })
  }
}

pub trait WorkerLoop {
  async fn start_loop(worker: Worker<Self>) -> WorkerTaskResult<()>;
}

#[derive(Clone)]
pub struct Worker<T : WorkerLoop + ?Sized> {
  run_info: SharedWorkerInfo,
  tasks: (mpsc::Sender<TaskRunInfo>, Arc<Mutex<mpsc::Receiver<TaskRunInfo>>>),
  done_tasks_counter: Arc<AtomicIsize>,
  on_counter: Arc<AtomicUsize>,
  stop_condition: Arc<Box<dyn Send + Sync + Fn(&Worker<T>) -> bool>>,
  task_sender: mpsc::Sender<TaskRunInfo>
}

const WORKER_TASKS_CAPACITY: usize = 10;

impl<T : WorkerLoop + ?Sized + Clone> Worker<T> {
  pub fn new(run_info: SharedWorkerInfo, task_sender: mpsc::Sender<TaskRunInfo>) -> Self {
    let (sender, receiver) = mpsc::channel(WORKER_TASKS_CAPACITY);
    Self {
      run_info,
      tasks: (sender, Arc::new(Mutex::new(receiver))),
      done_tasks_counter: Arc::new(AtomicIsize::new(-1)),
      on_counter: Arc::new(AtomicUsize::new(0)),
      stop_condition: Arc::new(Box::new(|_| false)),
      task_sender
    }
  }

  pub fn with_stop_condition<F>(
    run_info: SharedWorkerInfo,
    task_sender: mpsc::Sender<TaskRunInfo>,
    stop_condition: F
  ) -> Self where F : 'static + Send + Sync + Fn(&Worker<T>) -> bool
  {
    let (sender, receiver) = mpsc::channel(WORKER_TASKS_CAPACITY);
    Self {
      run_info,
      tasks: (sender, Arc::new(Mutex::new(receiver))),
      done_tasks_counter: Arc::new(AtomicIsize::new(-1)),
      on_counter: Arc::new(AtomicUsize::new(0)),
      stop_condition: Arc::new(Box::new(stop_condition)),
      task_sender
    }
  }

  pub fn done_tasks_count(&self) -> isize {
    self.done_tasks_counter.load(std::sync::atomic::Ordering::Relaxed)
  }

  pub async fn should_be_on(&self) -> bool {
    self.run_info.read().await.is_some()
  }

  pub fn is_on(&self) -> bool {
    self.on_counter.load(std::sync::atomic::Ordering::Relaxed) > 0
  }

  /// queue task for module execution
  pub async fn queue_task(&self, task: TaskRunInfo) {
    self.tasks.0.send(task).await.expect("queue task did not work");
  }

  /// queue task back to the service
  pub async fn service_queue_task(&self, task: TaskRunInfo) {
    self.task_sender.send(task).await.expect("service queue task did not work");
  }

  pub async fn next_task(&self) -> Option<(WorkerRunInfo, TaskRunInfo)> {
    if (self.stop_condition)(self) {
      return None
    }

    let task_info = loop {
      let task_info = self.tasks.1.lock().await.recv().await.expect("closed channel");
      if !task_info.stopped().await {
        break task_info
      }
    };

    let run_info = {
      match self.run_info.read().await.clone() {
        Some(run_info) => run_info,
        None => {
          self.tasks.0.send(task_info).await.expect("closed channel"); // prevent leaked task_infos
          return None
        }
      }
    };

    Some((run_info, task_info))
  }

  pub async fn worker_loop(self) {
    let on_counter = self.on_counter.clone();
    on_counter.fetch_add(1, Ordering::Relaxed);
    if let Err(err) = T::start_loop(self).await {
      if let Some(task_info) = err.task_info {

        let _ = task_info.status.send(if err.fail {
          let (duration_total, duration_execution) = task_info.get_duration_now();
          TaskStatus::Fail { reason: err.other_error.to_string(), duration_total, duration_execution }
        } else {
          TaskStatus::Error { reason: err.other_error.to_string() }
        });

      }
      error!("worker ended with error: {}", err.other_error.to_string());
    } else {
      debug!("worker ended correctly");
    }
    on_counter.fetch_sub(1, Ordering::Relaxed);
  }
}