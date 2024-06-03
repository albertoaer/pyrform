use std::{collections::{hash_map::Entry, HashMap}, fs as fs_sync, ops::DerefMut, path::{Path, PathBuf}, sync::Arc};

use itertools::Itertools;
use notify::{Config, RecommendedWatcher, Watcher};
use path_absolutize::Absolutize;
use tokio::{fs, sync::{mpsc, oneshot, watch, Mutex, RwLock}, time::Instant};
use tracing::debug;

use crate::model::{Task, TaskStatus, CreateWorkerData};

use super::{task::TaskRunInfo, worker::{SharedWorkerInfo, Worker, WorkerRunInfo}};

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
  workers_info: Arc<Mutex<HashMap<PathBuf, SharedWorkerInfo>>>,
  workers: Arc<Mutex<HashMap<String, Worker>>>,
}

impl WorkerService {
  /// TODO: allow recursive worker directory
  pub fn new(directory: impl AsRef<Path>) -> anyhow::Result<Self> {
    let directory = std::env::current_dir()?.join(directory.as_ref()).absolutize().unwrap().to_path_buf();

    let worker_files = fs_sync::read_dir(&directory)?.filter_map(|item| {
      let path = item.ok()?.path();
      if can_be_python_file(&path) {
        Some((path, Arc::new(RwLock::new(None))))
      } else {
        None
      }
    });

    let service = WorkerService {
      directory: Arc::new(directory),
      workers_info: Arc::new(Mutex::new(HashMap::from_iter(worker_files))),
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

        match (path.is_file(), self.workers_info.lock().await.entry(path.clone())) {
          (true, Entry::Vacant(vacant)) => { vacant.insert(Arc::new(RwLock::new(None))); }, // file create/renamed
          (false, Entry::Occupied(mut ocupied)) => {
            *ocupied.get_mut().write().await = None;
            ocupied.remove_entry();
          }, // file removed/renamed
          (true, Entry::Occupied(mut ocupied)) => { // file content changed
            if let some @ Some(_) = ocupied.get_mut().write().await.deref_mut() {
              let worker_name = get_worker_name(&path).expect("worker name");
              *some = Some(self.new_worker_run_info(&worker_name, Some(&path)).await.expect("run info"));
            }
          }
          (false, Entry::Vacant(_)) => { continue } // do nothing
        }

        debug!("updated: {:?}", path);
      }
    }
  }

  /// queues a task
  /// if `Ok` is returned with two channels: `oneshot::Sender<()>` is the signal to stop the task and
  /// `watch::Receiver<TaskStatus>` provides the current task status
  pub async fn queue_task(&self, task: Task) -> anyhow::Result<(oneshot::Sender<()>, watch::Receiver<TaskStatus>)> {
    let (status_tx, status_rx) = watch::channel(TaskStatus::Pending);
    let (stop_tx, stop_rx) = oneshot::channel();
    let result = Ok((stop_tx, status_rx));

    let worker_name = task.worker.clone();
    let dedicated = task.dedicated;
    let task_run_info = TaskRunInfo::new(task, stop_rx, status_tx);

    if dedicated { // create a worker for dedicated work
      let info = self.get_worker_shared_run_info(&worker_name).await?;
      let worker = Worker::with_stop_condition(
        info.clone(),
        |worker| worker.done_tasks_count() > 0 // will die after one task
      );
      worker.queue_task(task_run_info).await;

      tokio::spawn(worker.worker_loop());
      return result
    }

    { // the worker is found
      if let Entry::Occupied(mut worker) = self.workers.lock().await.entry(worker_name.clone()) {
        {
          let worker = worker.get_mut();
          if worker.should_be_on().await {
            worker.queue_task(task_run_info).await;
            return result
          }
        }
        worker.remove_entry(); // remove if it's not on
      }
    }

    { // create a new worker
      let info = self.get_worker_shared_run_info(&worker_name).await?;
      let worker = Worker::new(info.clone());
      worker.queue_task(task_run_info).await;
      
      tokio::spawn(worker.clone().worker_loop());
      self.workers.lock().await.insert(worker_name, worker);
    }

    result
  }

  pub async fn add_worker(&self, worker: CreateWorkerData) -> anyhow::Result<()> {
    let path = self.get_worker_path(&worker.name);
    if path.is_file() && !worker.replace {
      return Err(anyhow::anyhow!("the worker ({}) already exists and `replace` was negated", worker.name))
    }
    fs::write(path, worker.source).await?;
    Ok(())
  }

  pub async fn del_worker(&self, name: impl AsRef<str>) -> anyhow::Result<()> {
    let path = self.get_worker_path(&name);
    if !path.is_file() {
      return Err(anyhow::anyhow!("worker does not exists ({})", name.as_ref()))
    }
    fs::remove_file(path).await?;
    Ok(())
  }

  pub async fn get_worker(&self, name: impl AsRef<str>) -> anyhow::Result<String> {
    let path = self.get_worker_path(&name);
    if !path.is_file() {
      return Err(anyhow::anyhow!("worker does not exists ({})", name.as_ref()))
    }
    Ok(fs::read_to_string(path).await?)
  }

  pub fn get_worker_path(&self, name: impl AsRef<str>) -> PathBuf {
    self.directory.join(format!("{}.py", name.as_ref()))
  }

  pub async fn get_worker_shared_run_info(&self, name: impl AsRef<str>) -> anyhow::Result<SharedWorkerInfo> {
    let path = self.get_worker_path(&name);
    if let Some(info) = self.workers_info.lock().await.get(&path) {
      if let none @ None = info.write().await.deref_mut() {
        *none = Some(self.new_worker_run_info(&name, Some(path)).await?);
      }

      return Ok(info.clone())
    }
    return Err(anyhow::anyhow!("worker does not exists ({})", name.as_ref()))
  }

  pub async fn new_worker_run_info(
    &self, name: impl AsRef<str>, path: Option<impl AsRef<Path>>
  ) -> anyhow::Result<WorkerRunInfo> {
    let path = path.and_then(|x| Some(x.as_ref().into())).unwrap_or(self.get_worker_path(&name));
    let source = fs::read_to_string(&path).await?;
    Ok(WorkerRunInfo {
      worker_name: name.as_ref().to_string(),
      worker_path: path.to_string_lossy().to_string(),
      source,
      instant: Instant::now()
    })
  }
}