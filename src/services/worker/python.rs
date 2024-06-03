use std::future::Future;

use pyo3::{types::{PyAnyMethods, PyModule}, Py, Python};
use tokio::{runtime::Handle, task};

use crate::model::TaskStatus;

use super::{task::TaskRunInfo, worker::WorkerRunInfo};

pub async fn start_python_loop<F, O>(mut next: F) -> anyhow::Result<()>
  where F: (FnMut() -> O) + Send + 'static, O: Future<Output = Option<(WorkerRunInfo, TaskRunInfo)>>
{
  let handle = Handle::current();
  task::spawn_blocking(move || {
    Python::with_gil(|py| -> anyhow::Result<()> {
      let get_func = |run_info: &WorkerRunInfo| {
        let module = PyModule::from_code_bound(py, &run_info.source, &run_info.worker_path, &run_info.worker_name)?;
        module.getattr("worker")
      };

      let mut func = Python::None(py);
      let mut old_run_info: Option<WorkerRunInfo> = None;

      loop {
        let (run_info, task_info) = match py.allow_threads(|| handle.block_on(next())) {
          Some(values) => values,
          None => return Ok(())
        };

        let updated_run_info = old_run_info.and_then(|old| Some(old != run_info)).unwrap_or(true);
        if updated_run_info {
          func = get_func(&run_info)?.into();
        }
        old_run_info = Some(run_info.clone());

        task_info.set_status(TaskStatus::Running);

        let task = Py::new(py, task_info.task_for_python())?;
        let result = func.call1(py, (task, ));

        let _ = task_info.set_status(match result {
          Ok(_) => TaskStatus::Done,
          Err(_) => TaskStatus::Fail,
        });
      }
    })
  }).await??;
  Ok(())
}