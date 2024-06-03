use std::sync::{atomic::AtomicIsize, Arc};

use tokio::{sync::{mpsc, Mutex, RwLock}, time::Instant};
use tracing::debug;

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
  tasks: (mpsc::Sender<TaskRunInfo>, Arc<Mutex<mpsc::Receiver<TaskRunInfo>>>),
  done_tasks: Arc<AtomicIsize>,
  stop_condition: Arc<Box<dyn Send + Sync + Fn(&Worker) -> bool>>
}

const WORKER_TASKS_CAPACITY: usize = 10;

impl Worker {
  pub fn new(run_info: SharedWorkerInfo) -> Self {
    let (sender, receiver) = mpsc::channel(WORKER_TASKS_CAPACITY);
    Self {
      run_info,
      tasks: (sender, Arc::new(Mutex::new(receiver))),
      done_tasks: Arc::new(AtomicIsize::new(-1)),
      stop_condition: Arc::new(Box::new(|_| false))
    }
  }

  pub fn with_stop_condition<F>(run_info: SharedWorkerInfo, stop_condition: F) -> Self
    where F : 'static + Send + Sync + Fn(&Worker) -> bool
  {
    let (sender, receiver) = mpsc::channel(WORKER_TASKS_CAPACITY);
    Self {
      run_info,
      tasks: (sender, Arc::new(Mutex::new(receiver))),
      done_tasks: Arc::new(AtomicIsize::new(-1)),
      stop_condition: Arc::new(Box::new(stop_condition))
    }
  }

  pub fn done_tasks_count(&self) -> isize {
    self.done_tasks.load(std::sync::atomic::Ordering::Relaxed)
  }

  pub async fn should_be_on(&self) -> bool {
    self.run_info.read().await.is_some()
  }

  pub async fn queue_task(&self, task: TaskRunInfo) {
    let _ = self.tasks.0.send(task).await;
  }

  pub async fn worker_loop(self) {
    let _ = start_python_loop(move || {
      let worker = self.clone();
      worker.done_tasks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

      async move {
        if (worker.stop_condition)(&worker) {
          return None
        }

        let task = loop {
          let task = worker.tasks.1.lock().await.recv().await.expect("closed channel");
          if !task.stopped().await {
            break task
          }
        };

        let run_info = {
          match worker.run_info.read().await.clone() {
            Some(run_info) => run_info,
            None => {
              worker.tasks.0.send(task).await.expect("closed channel"); // prevent leaked tasks
              return None
            }
          }
        };

        debug!("about to run task with title: {:?}", task.task.title);

        Some((run_info, task))
      }
    }).await;
    debug!("worker ended");
  }
}