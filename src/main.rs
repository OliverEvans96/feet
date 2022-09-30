use std::path::{Path, PathBuf};

use clap::Parser;

use crate::config::Config;

mod config;

#[derive(Parser)]
struct Opts {
    #[arg(short, long)]
    config: Option<PathBuf>,
}

fn get_xdg_dirs() -> anyhow::Result<xdg::BaseDirectories> {
    xdg::BaseDirectories::with_prefix("feet").map_err(Into::into)
}

fn get_config<P: AsRef<Path>>(path: Option<P>) -> anyhow::Result<Config> {
    let xdg_dirs = get_xdg_dirs()?;
    let config_path = path
        .map(|p| p.as_ref().to_owned())
        .or_else(|| xdg_dirs.find_config_file("config.toml"));

    let parsed_config;
    if let Some(path) = config_path {
        let config_bytes = std::fs::read(path)?;
        parsed_config = toml::from_slice(&config_bytes)?;
    } else {
        parsed_config = Config::default()
    }

    Ok(parsed_config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let opts = Opts::parse();

    let parsed_config = get_config(opts.config)?;

    dbg!(parsed_config);

    Ok(())
}
