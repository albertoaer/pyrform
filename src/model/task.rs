use std::time::Duration;

use serde::{Serialize, Deserialize};
use serde_with::{serde_as, DurationSeconds};

fn task_function_default() -> String {
  "worker".into()
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
  pub worker: String,
  #[serde(default = "task_function_default")]
  pub function: String,
  #[serde(default)]
  pub title: Option<String>,
  #[serde(default)]
  pub dedicated: bool,
  #[serde(default)]
  pub args: Vec<String>,
  #[serde(default)]
  #[serde_as(as = "DurationSeconds<f64>")]
  pub delay: Duration,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status")]
pub enum TaskStatus {
  Pending, // awaiting to be run for first time
  Running, // doing work
  Done {
    outcome: String,
    duration_total: Duration,
    duration_execution: Duration,
    queue_again: bool
  }, // task finished
  Fail {
    reason: String,
    duration_total: Duration,
    duration_execution: Duration
  }, // failed during execution
  Error {
    reason: String
  }, // an error occurred and was not caused during task execution 
  Cancelled // the task has been cancelled (might have not ended the work or even started)
}

impl TaskStatus {
  pub fn can_be_cancelled(&self) -> bool {
    match &self {
      Self::Pending | Self::Running | Self::Done { queue_again: true, .. } => true,
      _ => false
    }
  }

  pub fn is_finished(&self) -> bool {
    match self {
      Self::Pending | Self::Running => false,
      _ => true
    }
  }

  pub fn is_queued_again(&self) -> bool {
    match self {
      Self::Done { queue_again, .. } if *queue_again => true,
      _ => false
    }
  }

  pub fn is_finished_forever(&self) -> bool {
    self.is_finished() && !self.is_queued_again()
  }
}