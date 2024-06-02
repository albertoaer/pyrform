use std::{collections::{hash_map::Entry, HashMap}, fs as fs_sync, path::{Path, PathBuf}, sync::Arc, time::Duration};

use itertools::Itertools;
use notify::{Config, RecommendedWatcher, Watcher};
use path_absolutize::Absolutize;
use tokio::{fs, sync::{broadcast, mpsc, oneshot, watch, Mutex}};

use crate::model::{Task, TaskStatus};

use super::{python::start_python_loop, task::TaskRunInfo};

// does not check if file exists only if the extension is valid
fn can_be_python_file(path: impl AsRef<Path>) -> bool {
  match path.as_ref().extension() {
    Some(ext) => ext.to_string_lossy() == "py",
    None => false
  }
}

fn get_worker_name(path: impl AsRef<Path>) -> Option<String> {
  Some(path.as_ref().file_stem()?.to_string_lossy().to_string())
}

#[derive(Clone)]
pub struct WorkerService {
  directory: Arc<PathBuf>, // must never change
  worker_files: Arc<Mutex<HashMap<PathBuf, Option<Arc<String>>>>>,
  workers: Arc<Mutex<HashMap<String, Worker>>>,
}

impl WorkerService {
  /// TODO: allow recursive worker directory
  pub fn new(directory: impl AsRef<Path>) -> anyhow::Result<Self> {
    let directory = std::env::current_dir()?.join(directory.as_ref()).absolutize().unwrap().to_path_buf();

    let worker_files = fs_sync::read_dir(&directory)?.filter_map(|item| {
      let path = item.ok()?.path();
      if can_be_python_file(&path) {
        Some((path, None::<Arc<String>>))
      } else {
        None
      }
    });

    let service = WorkerService {
      directory: Arc::new(directory),
      worker_files: Arc::new(Mutex::new(HashMap::from_iter(worker_files))),
      workers: Arc::new(Mutex::new(HashMap::new())),
    };

    tokio::spawn(service.clone().directory_watch_service());

    Ok(service)
  }

  async fn directory_watch_service(self) {
    let (tx, mut rx) = mpsc::channel(1);

    let mut watcher = RecommendedWatcher::new(move |res| {
      tx.blocking_send(res).expect("expected to deliver event");
    }, Config::default()).expect("expected watcher to work");
    let _ = watcher.watch(&self.directory, notify::RecursiveMode::NonRecursive);

    while let Some(Ok(res)) = rx.recv().await {
      for path in res.paths.into_iter().filter(|path| can_be_python_file(path)).unique() {
        if !can_be_python_file(&path) {
          continue;
        }

        match (path.is_file(), self.worker_files.lock().await.entry(path.clone())) {
          (true, Entry::Vacant(vacant)) => { vacant.insert(None); }, // file create/renamed
          (false, Entry::Occupied(ocupied)) => { ocupied.remove_entry(); }, // file removed/renamed
          (true, Entry::Occupied(mut ocupied)) => { ocupied.insert(None); } // file content changed
          (false, Entry::Vacant(_)) => { continue } // do nothing
        }

        println!("updated: {:?}", path);
        
        if let Some(name) = get_worker_name(&path) {
          if let Some(worker) = self.workers.lock().await.get(&name) {
            println!("updated worker: {:?}", name);

            let source = fs::read_to_string(&path).await.ok().map(|x| Arc::new(x));
            if let Some(source) = source.clone() {
              self.worker_files.lock().await.insert(path, Some(source));
            }

            worker.update_source(source).await;
          }
        }
      }
    }
  }

  pub async fn queue_task(&mut self, task: Task, stop: oneshot::Receiver<()>) -> anyhow::Result<watch::Receiver<TaskStatus>> {
    let (tx, rx) = watch::channel(TaskStatus::Pending);

    let worker_name = task.worker.clone();

    let task_run_info = TaskRunInfo {
      status: tx,
      task: task.into(),
      stop: Arc::new(Mutex::new(stop))
    };

    {
      if let Some(worker) = self.workers.lock().await.get(&worker_name) {
        worker.queue_task(task_run_info).await;
        return Ok(rx)
      }
    }

    if let Some(source) = self.worker_files.lock().await.get_mut(&self.get_worker_path(&worker_name)) {
      
      let source = if let Some(source) = source {
        source.clone()
      } else {
        *source = Some(Arc::new(fs::read_to_string(&self.get_worker_path(&worker_name)).await?));
        source.clone().unwrap()
      };

      let worker = Worker::new(source);
      
      tokio::spawn(worker.clone().worker_loop(task_run_info));
      self.workers.lock().await.insert(worker_name, worker);
      
    } else {
      return Err(anyhow::Error::msg(format!("unable to find the worker {}", worker_name)));
    }

    Ok(rx)
  }

  fn get_worker_path(&self, name: impl AsRef<str>) -> PathBuf {
    self.directory.join(format!("{}.py", name.as_ref()))
  }
}

#[derive(Clone)]
struct Worker {
  updates: watch::Sender<Option<Arc<String>>>,
  tasks: broadcast::Sender<TaskRunInfo>
}

const WORKER_TASKS_CAPACITY: usize = 10;

impl Worker {
  fn new(source: Arc<String>) -> Self {
    Self {
      updates: watch::Sender::new(Some(source)),
      tasks: broadcast::Sender::new(WORKER_TASKS_CAPACITY),
    }
  }

  async fn update_source(&self, source: Option<Arc<String>>) {
    let _ = self.updates.send(source);
  }

  async fn queue_task(&self, task: TaskRunInfo) {
    let _ = self.tasks.send(task);
  }
}

impl Worker {
  async fn worker_loop(self, task: TaskRunInfo) {
    let updates = Arc::new(Mutex::new(self.updates.subscribe()));
    let tasks = Arc::new(Mutex::new(self.tasks.subscribe()));

    let run_info = updates.lock().await.borrow_and_update().clone().expect("expecting worker source");

    let _ = start_python_loop(run_info, task, move || {
      let updates = updates.clone();
      let tasks = tasks.clone();

      async move {
        (
          updates.lock().await.borrow_and_update().clone(),
          Some(tasks.lock().await.recv().await.expect("expecting task"))
        )
      }
    }).await;
  }
}