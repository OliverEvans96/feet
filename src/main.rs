use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{Parser, Subcommand};
use gluesql::prelude::{Glue, Payload, Value};
use names::TableIdentifier;
use ptree::{item::StringItem, TreeBuilder};
use rustyline::error::ReadlineError;

// use gluesql::core::store::{GStore, GStoreMut};

use crate::config::Config;
use crate::glue::{TableData, TableNode};
use crate::names::TableName;

mod config;
mod glue;
mod line_injector;
mod names;

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
    /// List tables
    Tree { subdir: Option<String> },
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
        Payload::Create => println!("Created table"),
        Payload::Insert(n) => println!("Inserted {} rows", n),
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
        Payload::Delete(n) => println!("Deleted {} rows", n),
        Payload::Update(n) => println!("Updated {} rows", n),
        Payload::DropTable => println!("Dropped table."),
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
}

fn get_or_create_data_file(filename: &str) -> anyhow::Result<PathBuf> {
    let xdg_dirs = get_xdg_dirs()?;
    xdg_dirs
        .find_data_file(filename)
        .map(Ok)
        .unwrap_or_else(|| xdg_dirs.place_data_file(filename))
        .map_err(Into::into)
}

fn add_node_to_tree(
    store: &CsvStore,
    tree: &mut TreeBuilder,
    node: TableNode,
) -> anyhow::Result<()> {
    // Last component of name
    let mut last_name = node.name.last().unwrap_or("/".to_string());
    match node.data {
        TableData::Table(_) => {
            tree.add_empty_child(last_name);
        }
        TableData::Dir => {
            // TODO: don't parse schema for every file
            let subtables = store.list_tables(node.name)?;
            last_name.push('/');
            tree.begin_child(last_name);
            for subtable in subtables {
                add_node_to_tree(store, tree, subtable)?;
            }
            tree.end_child();
        }
    }

    Ok(())
}

fn build_table_tree(store: &CsvStore, sub_name: TableName) -> anyhow::Result<StringItem> {
    let tables = store.list_tables(sub_name.clone())?;

    let tree_title: TableIdentifier = sub_name.try_into()?;
    let mut tree = TreeBuilder::new(tree_title.to_string());

    for node in tables {
        add_node_to_tree(&store, &mut tree, node)?;
    }

    Ok(tree.build())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let config = get_config(opts.config.as_ref())?;

    // TODO: Parse during Opts::parse
    let data_dir = parse_data_dir(&config.data_dir)?;
    let history_file = get_or_create_data_file("history.txt")?;

    let store = CsvStore::new(config);
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
        Command::Tree { subdir } => {
            let sub_id = TableIdentifier::new(subdir.unwrap_or_default(), data_dir);
            let sub_name: TableName = sub_id.try_into()?;

            let store = glue.storage.expect("No underlying storage??");

            let tree = build_table_tree(&store, sub_name)?;

            ptree::print_tree(&tree)?;
        }
        Command::List { subdir } => {
            let sub_id = TableIdentifier::new(subdir.unwrap_or_default(), data_dir);
            let sub_name: TableName = sub_id.try_into()?;

            let store = glue.storage.expect("No underlying storage??");
            let tables = store.list_tables(sub_name)?;

            for node in tables {
                let table_id: TableIdentifier = node.name.try_into()?;
                match node.data {
                    TableData::Table(_) => println!("* {} ", &*table_id),
                    TableData::Dir => println!("* {}/ (directory)", &*table_id),
                }
            }
        }
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
