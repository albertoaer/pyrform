use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
  #[arg(short, long, default_value = "0.0.0.0:3000")]
  pub bind: String,

  #[arg(short, long, default_value_t = false, help = "expose the /worker endpoint")]
  pub edit_workers: bool,

  #[arg(default_value = "./workers", help = "workers' source files")]
  pub path: String
}