use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
  pub worker: String,
  #[serde(default)]
  pub title: Option<String>,
  #[serde(default)]
  pub dedicated: bool,
  #[serde(default)]
  pub args: Vec<String>
  // TODO: Cron
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TaskStatus {
  Pending, // awaiting to be run for first time
  Running, // doing work
  Scheduled, // not done, awaiting to be run
  Done, // no more work to do
  Fail, // failed during execution
  Cancelled // the task has been cancelled (might have not ended the work or even started)
}

impl TaskStatus {
  pub fn can_be_cancelled(&self) -> bool {
    matches!(self, Self::Pending | Self::Running | Self::Scheduled)
  }
}