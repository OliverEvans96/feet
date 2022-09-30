use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use gluesql::prelude::Glue;

use crate::config::Config;
use crate::glue::{TableName, TableNode};

mod config;
mod glue;

#[derive(Debug, Parser)]
struct Opts {
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Query data
    Query { query: String },
    /// List tables
    List { subdir: Option<String> },
    /// Show schema for a table
    Show { table_name: String },
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

/// Expand and canonicalize path
fn parse_data_dir(orig: &str) -> anyhow::Result<PathBuf> {
    let s = shellexpand::tilde(orig);
    let pb = PathBuf::from_str(&s)?;
    println!("AA");
    let can = pb.canonicalize()?;

    println!("BB");
    Ok(can)
}

fn list_tables(config: &Config) -> anyhow::Result<Vec<String>> {
    let expanded = shellexpand::tilde(&config.data_dir);
    // let data_dir = config.data_dir.canonicalize()?;
    let data_dir = expanded.to_string();
    let err_msg = format!("cannot open data_dir: {:?}", &data_dir);
    let read_dir = std::fs::read_dir(&data_dir).context(err_msg)?;

    let mut tables = Vec::new();

    for res in read_dir {
        match res {
            Ok(entry) => {
                let path = entry.path();
                let rel = path.strip_prefix(&data_dir)?;
                if let Ok(rel_str) = rel.to_owned().into_os_string().into_string() {
                    tables.push(rel_str)
                } else {
                    eprintln!("Non-UTF-8 path: {:?}", rel);
                }
            }
            Err(err) => {
                eprintln!("err 1: {}", err);
            }
        }
    }

    Ok(tables)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");

    let opts = Opts::parse();

    let config = get_config(opts.config.as_ref())?;

    dbg!(&opts);
    dbg!(&config);

    // TODO: Parse during Opts::parse
    let data_dir = parse_data_dir(&config.data_dir)?;

    println!("parsed data dir");

    let store = crate::glue::CsvStore::new(data_dir.clone());

    println!("created store");

    match opts.command {
        Command::Query { query } => todo!(),
        Command::List { subdir } => {
            let sub_name: TableName = subdir.map(|x| x.as_str().into()).unwrap_or_default();

            // let sub_name: TableName;
            // if let Some(dir) = subdir {
            //     let sub_pb: PathBuf = data_dir.join(dir);
            //     sub_name = TableName::from(subdir)
            // } else {
            //     sub_name = TableName::new();
            // }

            let tables = store.list_tables(&sub_name).await?;

            println!("tables: {:#?}", tables);

            // for table in tables {
            //     println!("- {}", table);
            // }
        }
        Command::Show { table_name } => {}
    }

    Ok(())
}
