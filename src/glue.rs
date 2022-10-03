use std::ffi::OsStr;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use async_trait::async_trait;
use csv::{StringRecord, StringRecordsIter};
use globset::Glob;
use gluesql::core::ast::ColumnDef;
use gluesql::core::data::{Key, Row, Schema};
use gluesql::core::result::{Error as GlueError, MutResult, Result as GlueResult};
use gluesql::core::store::{GStore, GStoreMut, RowIter, Store, StoreMut};
use gluesql::prelude::{DataType, Value};

use crate::config::Config;
use crate::names::{TableIdentifier, TableName, TablePath};

// use crate::config::Config;

pub struct CsvStore {
    data_dir: PathBuf,
    ignores: Vec<String>,
}

#[derive(Debug)]
pub enum TableData {
    Table(Schema),
    Dir,
}

#[derive(Debug)]
pub struct TableNode {
    pub name: TableName,
    pub data: TableData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ColumnType {
    Int,
    Float,
    String,
}

impl From<ColumnType> for DataType {
    fn from(col_type: ColumnType) -> Self {
        match col_type {
            ColumnType::Int => DataType::Int32,
            ColumnType::Float => DataType::Float,
            ColumnType::String => DataType::Text,
        }
    }
}

fn get_column_types_for_table(path: TablePath) -> anyhow::Result<Vec<(String, ColumnType)>> {
    let mut reader = csv::Reader::from_path(path.as_csv())?;

    let headers: Vec<_> = reader.headers()?.iter().map(ToString::to_string).collect();
    let col_types =
        determine_column_types(reader.records(), headers.len()).context("get col_types")?;

    let pairs = headers.into_iter().zip(col_types).collect();
    Ok(pairs)
}

/// Read the whole file to try to determine a suitable schema
fn read_schema(path: TablePath) -> anyhow::Result<Schema> {
    let col_pairs =
        get_column_types_for_table(path.clone()).context("getting column types for schema")?;

    let table_id: TableIdentifier = path.try_into().context("table id -> path")?;

    let mut schema = Schema {
        table_name: table_id.to_string(),
        column_defs: Vec::new(),
        indexes: Vec::new(),
    };

    for (col_name, col_type) in col_pairs {
        let col_def = ColumnDef {
            name: col_name,
            data_type: col_type.into(),
            options: Vec::new(),
        };

        schema.column_defs.push(col_def);
    }

    Ok(schema)
}

/// Given two equal-length lists of column types,
/// return a same-length list of the more general type in each position.
fn merge_column_types(first: &[ColumnType], second: &[ColumnType]) -> Vec<ColumnType> {
    first.iter().zip(second).map(|(&f, &s)| f.max(s)).collect()
}

/// Determine the minimum column type needed for each column
/// by brute-force reading every value
fn determine_column_types(
    records: StringRecordsIter<std::fs::File>,
    ncols: usize,
) -> anyhow::Result<Vec<ColumnType>> {
    let init: Vec<ColumnType> = std::iter::repeat(ColumnType::Int).take(ncols).collect();

    records
        .into_iter()
        .map(|res| {
            res.map(column_types_from_record)
                .map_err(Into::<anyhow::Error>::into)
        })
        .try_fold(init, reduce_column_types)
}

fn reduce_column_types(
    new_types: Vec<ColumnType>,
    agg: anyhow::Result<Vec<ColumnType>>,
) -> anyhow::Result<Vec<ColumnType>> {
    agg.map(|ctypes| merge_column_types(&ctypes, &new_types))
}

fn column_types_from_record(record: StringRecord) -> Vec<ColumnType> {
    record.into_iter().map(min_column_type).collect()
}

/// Determine the strictest column type that can represent a value
fn min_column_type(value: &str) -> ColumnType {
    if value.parse::<i64>().is_ok() {
        ColumnType::Int
    } else if value.parse::<f64>().is_ok() {
        ColumnType::Float
    } else {
        ColumnType::String
    }
}

impl TableNode {
    fn try_from_dir_entry(entry: DirEntry, data_dir: &Path) -> anyhow::Result<Self> {
        let ftype = entry.metadata()?.file_type();

        let path = TablePath::try_new(entry.path(), data_dir.to_owned())?;
        let name: TableName = path.clone().try_into()?;
        // let name = TableName::try_from_path(&entry.path(), data_dir)?;

        if ftype.is_dir() {
            let data = TableData::Dir;
            return Ok(TableNode { name, data });
        } else if ftype.is_file() && entry.path().extension() == Some(OsStr::new("csv")) {
            let schema = read_schema(path)?;
            let data = TableData::Table(schema);
            return Ok(TableNode { name, data });
        } else {
            bail!("{:?} is not a file or directory?", entry.path());
        }
    }
}

impl CsvStore {
    pub fn new(config: Config) -> Self {
        let expanded = shellexpand::tilde(&config.data_dir);
        let data_dir = expanded.to_string().into();
        Self {
            data_dir,
            ignores: config.ignores,
        }
    }

    pub fn should_ignore(&self, filename: &str) -> anyhow::Result<bool> {
        self.ignores
            .iter()
            .map(|ig| Ok(Glob::new(ig.as_str())?.compile_matcher().is_match(filename)))
            .try_fold(false, |acc, next| next.map(|x| acc || x))
    }

    pub fn list_tables(&self, dir: TableName) -> anyhow::Result<Vec<TableNode>> {
        // println!("list_tables: {:?}", dir);
        let dir_path: TablePath = dir.try_into()?;
        // println!("list_tables: {:?}", dir_path.clone().as_dir());
        // let dir_path = dir.to_bare_path(&self.data_dir);
        let mut tables = Vec::new();

        for entry_res in std::fs::read_dir(dir_path.as_dir())? {
            let entry = entry_res?;

            if !self.should_ignore(entry.file_name().to_str().expect("funny filename!"))? {
                let node = TableNode::try_from_dir_entry(entry, &self.data_dir)?;
                tables.push(node);
            }
        }

        Ok(tables)
    }
}

// struct CsvRowIter {}

// impl Iterator<Item=GlueResult<(Key, Row)>> for CsvRowIter {
//     type Item;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

fn value_from_str(val: &str, typ: ColumnType) -> anyhow::Result<Value> {
    let res = match typ {
        ColumnType::Int => Value::I32(val.parse()?),
        ColumnType::Float => Value::F64(val.parse()?),
        ColumnType::String => Value::Str(val.to_owned()),
    };

    Ok(res)
}

#[async_trait(?Send)]
impl Store for CsvStore {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id
            .try_into()
            .context("convert table id to path")
            .map_err(|err: anyhow::Error| GlueError::StorageMsg(err.to_string()))?;
        let schema = read_schema(path)
            .context("reading schema")
            .map_err(|err| GlueError::StorageMsg(err.to_string()))?;

        Ok(Some(schema))
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<Row>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> GlueResult<RowIter> {
        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id
            .try_into()
            .context("table id -> path")
            .map_err(|err: anyhow::Error| GlueError::StorageMsg(err.to_string()))?;

        let col_pairs = get_column_types_for_table(path.clone())
            .context("getting column types")
            .map_err(|err| GlueError::StorageMsg(err.to_string()))?;
        let col_types: Vec<_> = col_pairs.into_iter().map(|(_name, typ)| typ).collect();

        let reader = csv::Reader::from_path(path.as_csv())
            .context("opening csv reader")
            .map_err(|err| GlueError::StorageMsg(err.to_string()))?;

        // Loop over rows
        let records = reader.into_records();
        let unboxed_iter = records.into_iter().enumerate().map(move |(i, res)| {
            let record = res
                .context("reading csv record")
                .map_err(|err| GlueError::StorageMsg(err.to_string()))?;

            let key = Key::I32(i.try_into().expect("failed to convert key to i32"));
            // Loop over records in the row
            let rec_it = record.into_iter();

            let row_vec: Vec<_> = rec_it
                .zip(&col_types)
                .map(|(s, &typ)| value_from_str(s, typ))
                .collect::<anyhow::Result<Vec<_>>>()
                .context("reading csv value")
                .map_err(|err| GlueError::StorageMsg(err.to_string()))?;
            let row = Row(row_vec);

            let pair = (key, row);

            Ok(pair)
        });

        let iter: RowIter = Box::new(unboxed_iter);

        Ok(iter)
    }
}

async fn touch(path: &Path) -> std::io::Result<()> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .await
        .map(|_file| ())
}

#[async_trait(?Send)]
impl StoreMut for CsvStore {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        todo!()
    }

    async fn append_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        todo!()
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        todo!()
    }
}

impl GStore for CsvStore {}
impl GStoreMut for CsvStore {}
