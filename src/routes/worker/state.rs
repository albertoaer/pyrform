use crate::services::worker::WorkerService;

#[derive(Clone)]
pub struct WorkerState {
  pub worker_service: WorkerService,
}

impl From<&WorkerService> for WorkerState {
  fn from(worker_service: &WorkerService) -> Self {
    Self {
      worker_service: worker_service.clone()
    }
  }
}