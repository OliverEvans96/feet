use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use gluesql::prelude::{Glue, Payload, Value};
use rustyline::error::ReadlineError;

// use gluesql::core::store::{GStore, GStoreMut};

use crate::config::Config;
use crate::glue::TableName;

mod config;
mod glue;

use crate::glue::CsvStore;

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
    /// SQL repl
    Repl,
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

fn print_payload(payload: Payload) {
    match payload {
        Payload::ShowColumns(cols) => {
            print!("SHOW COLUMNS: ");
            if let Some((last, most)) = cols.split_last() {
                for col in most {
                    print!("{} ({}), ", col.0, col.1);
                }

                println!("{} ({})", last.0, last.1);
            }
        }
        Payload::Create => todo!(),
        Payload::Insert(_) => todo!(),
        Payload::Select { labels, rows } => {
            let mut table_builder = tabled::builder::Builder::new();
            table_builder.set_columns(labels);
            for row in rows {
                table_builder.add_record(row.into_iter().map(format_value));
            }

            let mut table = table_builder.build();

            table.with(tabled::style::Style::modern());

            println!("{}", table);
        }
        Payload::Delete(_) => todo!(),
        Payload::Update(_) => todo!(),
        Payload::DropTable => todo!(),
    }
}

async fn handle_query(glue: &mut Glue<CsvStore>, query: &str) {
    let statements = glue.plan(query).await.expect("planning");

    for statement in statements {
        let payload = glue
            .execute_stmt_async(&statement)
            .await
            .expect("oops - glue");

        print_payload(payload);
    }

    // println!("{} responses: {:#?}", responses.len(), responses);

    // for payload in responses {
    //     print_payload(payload);
    // }
}

fn get_or_create_data_file(filename: &str) -> anyhow::Result<PathBuf> {
    let xdg_dirs = get_xdg_dirs()?;
    xdg_dirs
        .find_data_file(filename)
        .map(Ok)
        .unwrap_or_else(|| xdg_dirs.place_data_file(filename))
        .map_err(Into::into)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let config = get_config(opts.config.as_ref())?;

    // TODO: Parse during Opts::parse
    let data_dir = parse_data_dir(&config.data_dir)?;
    let history_file = get_or_create_data_file("history.txt")?;

    let store = CsvStore::new(data_dir.clone());
    let mut glue = Glue::new(store);

    match opts.command {
        Command::Repl => {
            let mut repl = rustyline::Editor::<()>::new()?;
            if repl.load_history(&history_file).is_err() {
                println!("No previous history.");
            }
            loop {
                let readline = repl.readline("> ");

                match readline {
                    Ok(query) => {
                        repl.add_history_entry(query.as_str());
                        handle_query(&mut glue, &query).await;
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        break;
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                        break;
                    }
                }
            }
            repl.save_history(&history_file)?;
        }
        Command::Query { query } => handle_query(&mut glue, &query).await,
        Command::List { subdir } => {
            let sub_name: TableName = subdir.map(|x| x.as_str().into()).unwrap_or_default();

            // let sub_name: TableName;
            // if let Some(dir) = subdir {
            //     let sub_pb: PathBuf = data_dir.join(dir);
            //     sub_name = TableName::from(subdir)
            // } else {
            //     sub_name = TableName::new();
            // }

            let store = glue.storage.expect("No underlying storage??");
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
        Value::Bool(x) => format!("{}", x),
        Value::I8(x) => format!("{}", x),
        Value::I16(x) => format!("{}", x),
        Value::I32(x) => format!("{}", x),
        Value::I64(x) => format!("{}", x),
        Value::I128(x) => format!("{}", x),
        Value::F64(x) => format!("{}", x),
        Value::Decimal(x) => format!("{}", x),
        Value::Bytea(x) => format!("{:?}", x),
        Value::Date(_) => todo!(),
        Value::Timestamp(_) => todo!(),
        Value::Time(_) => todo!(),
        Value::Interval(_) => todo!(),
        Value::Uuid(_) => todo!(),
        Value::Map(_) => todo!(),
        Value::List(_) => todo!(),
        Value::Null => "NULL".to_string(),
    }
}
