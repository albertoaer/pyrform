use std::future::Future;

use pyo3::{types::{PyAnyMethods, PyModule, PyModuleMethods}, Py, Python};
use tokio::{runtime::Handle, task};
use tracing::info;

use crate::model::TaskStatus;

use super::{task::{Task, TaskRunInfo}, worker::WorkerRunInfo};

pub async fn start_python_loop<F, O>(mut next: F) -> anyhow::Result<()>
  where F: (FnMut() -> O) + Send + 'static, O: Future<Output = Option<(WorkerRunInfo, TaskRunInfo)>>
{
  let handle = Handle::current();

  task::spawn_blocking(move || {
    Python::with_gil(|py| -> anyhow::Result<()> {
      let mut module = PyModule::new_bound(py, "empty module")?;
      let mut old_run_info: Option<WorkerRunInfo> = None;

      loop {
        let (run_info, mut task_info) = match py.allow_threads(|| handle.block_on(next())) {
          Some(values) => values,
          None => return Ok(())
        };

        let updated_run_info = old_run_info.and_then(|old| Some(old != run_info)).unwrap_or(true);
        if updated_run_info {
          module = PyModule::from_code_bound(
            py,
            &run_info.source,
            &run_info.worker_path,
            &run_info.worker_name
          )?;
          module.add_class::<Task>()?;
        }
        old_run_info = Some(run_info.clone());

        task_info.set_status(TaskStatus::Running);

        let task = Py::new(py, task_info.task_for_python())?;
        task_info.set_execution_now();
        let result = module.getattr(task_info.task.function.as_str())?.call1((&task, ));
        let (duration_total, duration_execution) = task_info.get_duration_now();

        if task.borrow(py).is_prevent_done() {
          info!("task should be queued again")
        }

        let _ = task_info.set_status(match result {
          Ok(outcome) => TaskStatus::Done {
            outcome: outcome.to_string(), duration_total, duration_execution
          },
          Err(reason) => TaskStatus::Fail {
            reason: reason.to_string(), duration_total, duration_execution
          },
        });
      }
    })
  }).await??;
  Ok(())
}