use std::collections::HashMap;
use std::fs::{DirEntry, File, FileType};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use async_trait::async_trait;
use csv::{StringRecord, StringRecordIter, StringRecordsIter};
use gluesql::core::ast::ColumnDef;
use gluesql::core::data::{Key, Row, Schema};
use gluesql::core::store::{RowIter, Store};

use gluesql::core::result::Result as GlueResult;
use gluesql::prelude::DataType;

use crate::config::Config;

pub struct CsvStore {
    data_dir: PathBuf,
}

#[derive(Debug)]
pub enum TableData {
    Table(Schema),
    Dir,
}

#[derive(Debug)]
pub struct TableNode {
    name: TableName,
    data: TableData,
}

/// Hierarchical (e.g. slash-delimited) path/name system
#[derive(Debug, Default)]
pub struct TableName(Vec<String>);

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("/"))
    }
}

impl From<&str> for TableName {
    fn from(s: &str) -> Self {
        Self(s.split("/").map(ToString::to_string).collect())
    }
}

impl TableName {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn try_from_path(path: &Path, data_dir: &Path) -> anyhow::Result<Self> {
        let path_can = path
            .canonicalize()
            .context(format!("canonicalize path: {:?}", path))?;
        let root_can = data_dir.canonicalize().context("canonicalize data_dir")?;

        let rel = path_can.strip_prefix(&root_can).context(format!(
            "path ({:?}) must be in data directory ({:?}).",
            path_can, root_can
        ))?;

        let no_ext = rel.with_extension("");

        let parts: Vec<String> = no_ext
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();

        Ok(Self(parts))
    }

    pub fn to_path(&self, data_dir: &Path) -> PathBuf {
        // Start from `data_dir`
        let mut path = data_dir.to_owned();

        // Add each component
        for part in &self.0 {
            path.push(part.clone());
        }

        path
    }
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

/// Read the whole file to try to determine a suitable schema
fn read_schema(path: &Path, data_dir: &Path) -> anyhow::Result<Schema> {
    let mut reader = csv::Reader::from_path(path)?;

    let name = TableName::try_from_path(path, data_dir)?;

    println!("read_schema: {}", name);

    let mut schema = Schema {
        table_name: name.to_string(),
        column_defs: Vec::new(),
        indexes: Vec::new(),
    };

    let headers: Vec<String> = reader.headers()?.iter().map(ToString::to_string).collect();
    let col_types = determine_column_types(reader.records(), headers.len())?;

    for (col_name, col_type) in headers.into_iter().zip(col_types) {
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
    records: StringRecordsIter<File>,
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

        let name = TableName::try_from_path(&entry.path(), data_dir)?;

        if ftype.is_dir() {
            let data = TableData::Dir;
            return Ok(TableNode { name, data });
        } else if ftype.is_file() {
            let schema = read_schema(&entry.path(), data_dir)?;
            let data = TableData::Table(schema);
            return Ok(TableNode { name, data });
        } else {
            bail!("{:?} is not a file or directory?", entry.path());
        }
    }
}

impl CsvStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub async fn list_tables(&self, dir: &TableName) -> anyhow::Result<Vec<TableNode>> {
        println!("list_tables: dir={:?}", dir);
        let dir_path = dir.to_path(&self.data_dir);
        let mut tables = Vec::new();

        for entry_res in std::fs::read_dir(dir_path)? {
            let entry = entry_res?;

            let node = TableNode::try_from_dir_entry(entry, &self.data_dir)?;
            tables.push(node);
        }

        Ok(tables)
    }

    async fn read_table(&self, table_name: &str) -> Schema {
        todo!()
    }
}

#[async_trait(?Send)]
impl Store for CsvStore {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        todo!()
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<Row>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> GlueResult<RowIter> {
        todo!()
    }
}
