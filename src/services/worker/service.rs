use std::{collections::{hash_map::Entry, HashMap}, fs as fs_sync, ops::DerefMut, path::{Path, PathBuf}, sync::Arc, time::Duration};

use futures::future::join_all;
use itertools::Itertools;
use notify::{Config, RecommendedWatcher, Watcher};
use path_absolutize::Absolutize;
use tokio::{fs, select, sync::{mpsc, oneshot, watch, Mutex, RwLock}, time::{self, Instant}};
use tracing::{debug, info, trace};

use crate::model::{Task, TaskStatus, CreateWorkerData};

use super::{python::PythonWorkerLoop, task::TaskRunInfo, worker::{SharedWorkerInfo, Worker, WorkerBuilder, WorkerRunInfo}};

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
  task_sender: mpsc::Sender<TaskRunInfo>
}

const SERVICE_TASKS_CAPACITY: usize = 1000;
const WORKER_MAX_RETRIES: u8 = 3;

impl WorkerService {
  /// TODO: allow recursive worker directory
  pub fn new(directory: impl AsRef<Path>) -> anyhow::Result<Self> {
    let directory = std::env::current_dir()?.join(directory.as_ref()).absolutize().unwrap().to_path_buf();

    let worker_files = fs_sync::read_dir(&directory)?.filter_map(|item| {
      let path = item.ok()?.path();
      if can_be_python_file(&path) {
        info!("Loaded worker: {:?}", path.file_name().unwrap_or(path.as_os_str()));
        Some((path, Arc::new(RwLock::new(None))))
      } else {
        None
      }
    });

    let (task_sender, task_receiver) = mpsc::channel(SERVICE_TASKS_CAPACITY);

    let service = WorkerService {
      directory: Arc::new(directory),
      workers_info: Arc::new(Mutex::new(HashMap::from_iter(worker_files))),
      workers: Arc::new(Mutex::new(HashMap::new())),
      task_sender
    };

    tokio::spawn(service.clone().directory_watch_service());
    tokio::spawn(service.clone().scheduler_service(task_receiver));

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
          (true, Entry::Vacant(vacant)) => {
            info!("Loaded worker: {:?}", path.file_name().unwrap_or(path.as_os_str()));
            vacant.insert(Arc::new(RwLock::new(None)));
          }, // file create/renamed
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

  async fn scheduler_service(self, mut task_receiver: mpsc::Receiver<TaskRunInfo>) {
    let mut delayed_tasks: Vec<TaskRunInfo> = Vec::new();
    let min_delay = time::sleep(Duration::MAX);

    tokio::pin!(min_delay);

    loop {
      select! {
        task = task_receiver.recv() => match task {
          Some(task_info) if task_info.task.delay.is_zero() => self.launch_task(task_info).await,
          Some(task_info) => {
            let idx = delayed_tasks.binary_search_by(|item| item.task.delay.cmp(&task_info.task.delay))
              .unwrap_or_else(|e| e);
            if idx == 0 {
              min_delay.as_mut().reset(Instant::now() + task_info.task.delay);
            }
            delayed_tasks.insert(idx, task_info);
          }
          None => {
            info!("error receiving task");
            break;
          }
        },
        () = &mut min_delay => {
          let now = Instant::now();
          let (ready, delayed): (Vec<_>, Vec<_>) = delayed_tasks.into_iter()
            .partition(|item| item.creation + item.task.delay < now);
          trace!("ready: {}, delayed: {}", ready.len(), delayed.len());

          delayed_tasks = delayed;

          min_delay.as_mut().reset(
            Instant::now() + delayed_tasks.first()
              .and_then(|x| Some(x.task.delay)).unwrap_or(Duration::from_secs(86400 * 365 * 30))
          );
          join_all(ready.into_iter().map(|x| self.launch_task(x))).await;
        }
      }
    }
  }

  async fn launch_task(&self, task_info: TaskRunInfo) {
    let status = task_info.status.clone();
    if let Err(err) = self.perform_launch_task(task_info).await {
      status.send(TaskStatus::Error { reason: err.to_string() }).expect("send status did not work");
    }
  }

  async fn perform_launch_task(&self, task_info: TaskRunInfo) -> anyhow::Result<()> {
    let task = task_info.task.clone();

    if task.dedicated { // create a worker for dedicated work
      let info = self.get_worker_shared_run_info(&task.worker).await?;
      let worker: Worker = WorkerBuilder::new()
        .run_info(info.clone())
        .task_sender(self.task_sender.clone())
        .stop_condition(|worker| worker.done_tasks_count() > 0) // will die after one task
        .build();
      worker.queue_task(task_info).await;

      tokio::spawn(worker.worker_loop::<PythonWorkerLoop>());
      return Ok(())
    }

    { // the worker is found
      if let Entry::Occupied(mut worker) = self.workers.lock().await.entry(task.worker.clone()) {
        {
          let worker = worker.get_mut();
          if worker.should_be_on().await && worker.is_on() {
            worker.queue_task(task_info).await;
            return Ok(())
          }
        }
        worker.remove_entry(); // remove if it's not on
      }
    }

    { // create a new worker
      let info = self.get_worker_shared_run_info(&task.worker).await?;
      let worker = WorkerBuilder::new()
        .run_info(info.clone())
        .task_sender(self.task_sender.clone())
        .retries(WORKER_MAX_RETRIES)
        .build();
      worker.queue_task(task_info).await;
      
      tokio::spawn(worker.clone().worker_loop::<PythonWorkerLoop>());
      self.workers.lock().await.insert(task.worker.clone(), worker);
    }

    Ok(())
  }

  /// queues a task
  /// if `Ok` is returned with two channels: `oneshot::Sender<()>` is the signal to stop the task and
  /// `watch::Receiver<TaskStatus>` provides the current task status
  pub async fn queue_task(&self, task: Task) -> anyhow::Result<(oneshot::Sender<()>, watch::Receiver<TaskStatus>)> {
    let (status_tx, status_rx) = watch::channel(TaskStatus::Pending);
    let (stop_tx, stop_rx) = oneshot::channel();
    let result = Ok((stop_tx, status_rx));

    let task_info = TaskRunInfo::new(task, stop_rx, status_tx);
    self.task_sender.send(task_info).await?;

    result
  }

  pub async fn add_worker(&self, worker: CreateWorkerData) -> anyhow::Result<()> {
    let path = self.get_worker_path(&worker.name);
    if path.is_file() && !worker.replace {
      return Err(anyhow::anyhow!("the worker ({}) already exists. Use 'replace' to force the operation", worker.name))
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
      name: name.as_ref().to_string(),
      path: path.to_string_lossy().to_string(),
      source,
      instant: Instant::now()
    })
  }
}