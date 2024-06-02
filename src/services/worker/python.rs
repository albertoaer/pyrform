use std::{future::Future, sync::Arc};

use pyo3::{types::{PyAnyMethods, PyModule}, Py, Python};
use tokio::{runtime::Handle, task};

use crate::model::TaskStatus;

use super::task::TaskRunInfo;

#[derive(Clone)]
pub struct PythonRunInfo {
  pub source: Arc<String>,
  pub worker_path: Arc<String>,
  pub worker_name: Arc<String>,
}

pub async fn start_python_loop<F, O>(mut source: Arc<String>, mut task_info: TaskRunInfo, mut next: F) -> anyhow::Result<()>
  where F: (FnMut() -> O) + Send + 'static, O: Future<Output = (Option<Arc<String>>, Option<TaskRunInfo>)>
{
  let handle = Handle::current();
  task::spawn_blocking(move || {
    Python::with_gil(|py| -> anyhow::Result<()> {
      let _ = task_info.status.send(TaskStatus::Running);

      let get_func = |source: &Arc<String>| {
        let module = PyModule::from_code_bound(py, &source, "TODO: file", "TODO: module")?;
        module.getattr("worker")
      };

      let mut func = get_func(&source)?;

      loop {
        let task = Py::new(py, task_info.task.clone())?;
        let result = func.call1((task, ));
        let _ = task_info.status.send(match result {
          Ok(_) => TaskStatus::Done,
          Err(_) => TaskStatus::Fail,
        });


        let old_source = source.clone();
        let (new_source, new_task_info) = handle.block_on(next());

        task_info = match new_task_info {
          Some(task_info) => task_info,
          None => break
        };
        if let Some(new_source) = new_source {
          if Arc::ptr_eq(&new_source, &old_source) {
            func = get_func(&new_source)?;
          }
          source = new_source;
        } else {
          break
        }
      }
      Ok(())
    })
  }).await??;
  Ok(())
}