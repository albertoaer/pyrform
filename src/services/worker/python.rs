use pyo3::{types::{PyAnyMethods, PyModule, PyModuleMethods}, Py, Python};
use tokio::{runtime::Handle, task};
use tracing::debug;

use crate::model::{self, TaskStatus};

use super::{task::Task, worker::{WorkerLoop, WorkerRunInfo}};

#[derive(Clone, Copy)]
pub struct PythonWorkerLoop;

impl WorkerLoop for PythonWorkerLoop {
  async fn start_loop(worker: super::worker::Worker<Self>) -> anyhow::Result<()> {
    let handle = Handle::current();

    task::spawn_blocking(move || {
      Python::with_gil(|py| -> anyhow::Result<()> {
        let mut module = PyModule::new_bound(py, "empty module")?;
        let mut old_run_info: Option<WorkerRunInfo> = None;

        loop {
          let (run_info, mut task_info) = match py.allow_threads(|| handle.block_on(worker.next_task())) {
            Some(values) => values,
            None => return Ok(())
          };

          debug!("[worker:{}] next task with title: {:?}", run_info.name, task_info.task.title);

          let updated_run_info = old_run_info.and_then(|old| Some(old != run_info)).unwrap_or(true);
          if updated_run_info {
            module = PyModule::from_code_bound(
              py,
              &run_info.source,
              &run_info.path,
              &run_info.name
            )?;
            module.add_class::<Task>()?;
          }
          old_run_info = Some(run_info.clone());

          task_info.set_status(TaskStatus::Running);

          let task = Py::new(py, task_info.task_for_python())?;
          task_info.set_execution_now();
          let result = module.getattr(task_info.task.function.as_str())?.call1((&task, ));
          let (duration_total, duration_execution) = task_info.get_duration_now();

          let queue_again = task.borrow(py).is_queue_again();

          let _ = task_info.set_status(match result {
            Ok(outcome) => TaskStatus::Done {
              outcome: outcome.to_string(), duration_total, duration_execution, queue_again
            },
            Err(reason) => TaskStatus::Fail {
              reason: reason.to_string(), duration_total, duration_execution
            },
          });

          if queue_again {
            let extracted_task: model::Task = task.extract::<Task>(py)?.into();
            py.allow_threads(|| {
              let mut updated_task_info = task_info.replace_task(extracted_task);
              updated_task_info.set_creation_now();
              handle.block_on(worker.service_queue_task(updated_task_info));
            });
          }
        }
      })
    }).await??;
    Ok(())
  }
}