use std::path::Path;

use pyo3::{types::{PyAnyMethods, PyList, PyListMethods, PyModule, PyModuleMethods}, Bound, Py, Python};
use tokio::{runtime::Handle, task};
use tracing::debug;

use crate::model::{self, TaskStatus};

use super::{task::Task, worker::{IntoWorkerTaskResult, Worker, WorkerLoop, WorkerRunInfo, WorkerTaskResult}};

#[derive(Clone, Copy)]
pub struct PythonWorkerLoop;

fn module_from_run_info<'py>(py: Python<'py>, run_info: &WorkerRunInfo) -> anyhow::Result<Bound<'py, PyModule>> {
  let module = PyModule::from_code_bound(
    py,
    &run_info.source,
    &run_info.path,
    &run_info.name
  )?;
  module.add_class::<Task>()?;
  Ok(module)
}

fn config_path(py: Python, run_info: &WorkerRunInfo) -> WorkerTaskResult<()> {
  let path: &Path = run_info.path.as_ref();
  if let Some(path_parent) = path.parent() {
    let syspath = py.import_bound("sys")
      .and_then(|sys| sys.getattr("path"))
      .and_then(|list| Ok(list.downcast_into::<PyList>().unwrap()))
      .worker_task_error(Some(run_info), None)?;

    if !syspath.contains(path_parent).worker_task_error(Some(run_info), None)? {
      syspath.append(path_parent).worker_task_error(Some(run_info), None)?;
    }
  }
  Ok(())
}

impl WorkerLoop for PythonWorkerLoop {
  async fn start_loop(worker: Worker<Self>) -> WorkerTaskResult<()> {
    let handle = Handle::current();

    task::spawn_blocking(move || {
      Python::with_gil(|py| -> WorkerTaskResult<()> {
        let (mut run_info, mut task_info) = match py.allow_threads(|| handle.block_on(worker.next_task())) {
          Some(values) => values,
          None => return Ok(())
        };

        config_path(py, &run_info)?;
        let mut module = module_from_run_info(py, &run_info).worker_task_error(Some(&run_info), Some(&task_info))?;

        loop {
          debug!("[worker:{}] next task with title: {:?}", run_info.name, task_info.task.title);

          task_info.set_status(TaskStatus::Running);

          let task = Py::new(py, task_info.task_for_python())
            .worker_task_error(Some(&run_info), Some(&task_info))?;

          let function = module.getattr(task_info.task.function.as_str())
            .worker_task_error(Some(&run_info), Some(&task_info))?;
          
          task_info.set_execution_now();
          let outcome = function.call1((&task, )).worker_task_fail(Some(&run_info), Some(&task_info))?;
          let (duration_total, duration_execution) = task_info.get_duration_now();

          let queue_again = task.borrow(py).is_queue_again();
          let _ = task_info.set_status(TaskStatus::Done {
            outcome: outcome.to_string(), duration_total, duration_execution, queue_again
          });

          let queue_again = if queue_again {
            let extracted_task: model::Task = task.extract::<Task>(py)
              .worker_task_error(Some(&run_info), Some(&task_info))?.into();
            let mut updated_task_info = task_info.replace_task(extracted_task);
            updated_task_info.set_creation_now();
            Some(updated_task_info)
          } else { None };

          let next = py.allow_threads(|| handle.block_on(async {
            if let Some(queue_again) = queue_again {
              worker.service_queue_task(queue_again).await;
            }
          
            worker.next_task().await
          }));
          match next {
            Some((new_run_info, new_task_info)) => {
              task_info = new_task_info;
            
              if new_run_info != run_info {
                run_info = new_run_info;
                config_path(py, &run_info)?;
                module = module_from_run_info(py, &run_info).worker_task_error(Some(&run_info), Some(&task_info))?;
              }
            },
            None => return Ok(())
          }
        }
      })
    }).await.worker_task_error(None, None)??;
    Ok(())
  }
}