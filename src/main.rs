use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use gluesql::prelude::{execute, parse, translate, Glue, Value};

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
    let can = pb.canonicalize()?;
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
    let opts = Opts::parse();

    let config = get_config(opts.config.as_ref())?;

    // TODO: Parse during Opts::parse
    let data_dir = parse_data_dir(&config.data_dir)?;

    let store = crate::glue::CsvStore::new(data_dir.clone());

    match opts.command {
        Command::Query { query } => {
            let mut glue = Glue::new(store);

            let responses = glue.execute(query).expect("oops - glue");

            for payload in responses {
                match payload {
                    gluesql::prelude::Payload::ShowColumns(cols) => {
                        print!("SHOW COLUMNS: ");
                        if let Some((last, most)) = cols.split_last() {
                            for col in most {
                                print!("{} ({}), ", col.0, col.1);
                            }

                            println!("{} ({})", last.0, last.1);
                        }
                    }
                    gluesql::prelude::Payload::Create => todo!(),
                    gluesql::prelude::Payload::Insert(_) => todo!(),
                    gluesql::prelude::Payload::Select { labels, rows } => {
                        let mut table_builder = tabled::builder::Builder::new();
                        table_builder.set_columns(labels);
                        for row in rows {
                            table_builder.add_record(row.into_iter().map(format_value));
                        }

                        let mut table = table_builder.build();

                        table.with(tabled::style::Style::modern());

                        println!("{}", table);
                    }
                    gluesql::prelude::Payload::Delete(_) => todo!(),
                    gluesql::prelude::Payload::Update(_) => todo!(),
                    gluesql::prelude::Payload::DropTable => todo!(),
                }
            }
        }
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

fn format_value(value: Value) -> String {
    match value {
        Value::Str(s) => s,
        Value::Bool(_) => todo!(),
        Value::I8(_) => todo!(),
        Value::I16(_) => todo!(),
        Value::I32(_) => todo!(),
        Value::I64(_) => todo!(),
        Value::I128(_) => todo!(),
        Value::F64(_) => todo!(),
        Value::Decimal(_) => todo!(),
        Value::Bytea(_) => todo!(),
        Value::Date(_) => todo!(),
        Value::Timestamp(_) => todo!(),
        Value::Time(_) => todo!(),
        Value::Interval(_) => todo!(),
        Value::Uuid(_) => todo!(),
        Value::Map(_) => todo!(),
        Value::List(_) => todo!(),
        Value::Null => todo!(),
    }
}
