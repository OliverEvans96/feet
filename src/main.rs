use std::path::PathBuf;

use clap::Parser;

use crate::config::Config;

mod config;

#[derive(Parser)]
struct Opts {
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let opts = Opts::parse();

    let xdg_dirs = xdg::BaseDirectories::with_prefix("feet")?;

    let config_path = opts
        .config
        .or_else(|| xdg_dirs.find_config_file("config.toml"));

    let parsed_config;
    if let Some(path) = config_path {
        let config_bytes = std::fs::read(path)?;
        parsed_config = toml::from_slice(&config_bytes)?;
    } else {
        parsed_config = Config::default()
    }

    dbg!(parsed_config);

    Ok(())
}
