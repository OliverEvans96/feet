use std::ffi::OsStr;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
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
use crate::format_value;
use crate::line_injector::{Injection, LineInjector};
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
        let dir_path: TablePath = dir.try_into()?;
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

fn get_i32_key(key: &Key) -> anyhow::Result<i32> {
    match *key {
        Key::I32(x) => Ok(x),
        _ => bail!("non-i32 key {:?}", key),
    }
}

fn get_row_num(key: &Key) -> anyhow::Result<usize> {
    match get_i32_key(key) {
        Ok(i) => match i.try_into() {
            Ok(row_num) => Ok(row_num),
            Err(_err) => bail!("Invalid row number {}", i),
        },
        Err(err) => Err(err),
    }
}

#[async_trait(?Send)]
impl Store for CsvStore {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id
            .try_into()
            .context("convert table id to path")
            .to_glue_err()?;
        if path.clone().as_csv().exists() {
            let schema = read_schema(path).context("reading schema").to_glue_err()?;

            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<Row>> {
        println!("fetch_data");
        dbg!(table_name);
        dbg!(key);

        // Number of rows to skip
        let nskip = get_row_num(key).to_glue_err()?;

        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id
            .try_into()
            .context("table id -> path")
            .to_glue_err()?;

        let col_pairs = get_column_types_for_table(path.clone())
            .context("getting column types")
            .to_glue_err()?;
        let col_types: Vec<_> = col_pairs.into_iter().map(|(_name, typ)| typ).collect();

        let reader = csv::Reader::from_path(path.as_csv())
            .context("opening csv reader")
            .to_glue_err()?;

        // Skip first n records
        let mut records = reader.into_records().skip(nskip);

        records
            .next()
            .map(|res| {
                let record = res.context("reading csv record").to_glue_err()?;
                let row = read_csv_record(record, col_types.clone())?;
                Ok(row)
            })
            .transpose()
    }

    async fn scan_data(&self, table_name: &str) -> GlueResult<RowIter> {
        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id
            .try_into()
            .context("table id -> path")
            .to_glue_err()?;

        let col_pairs = get_column_types_for_table(path.clone())
            .context("getting column types")
            .to_glue_err()?;
        let col_types: Vec<_> = col_pairs.into_iter().map(|(_name, typ)| typ).collect();

        let reader = csv::Reader::from_path(path.as_csv())
            .context("opening csv reader")
            .to_glue_err()?;

        // Loop over rows
        let records = reader.into_records();
        let unboxed_iter = records.into_iter().enumerate().map(move |(i, res)| {
            let key = Key::I32(i.try_into().expect("failed to convert key to i32"));
            let record = res.context("reading csv record").to_glue_err()?;
            let row = read_csv_record(record, col_types.clone())?;
            Ok((key, row))
        });

        let iter: RowIter = Box::new(unboxed_iter);

        Ok(iter)
    }
}

fn read_csv_record(record: StringRecord, col_types: Vec<ColumnType>) -> GlueResult<Row> {
    // Loop over records in the row
    let rec_it = record.into_iter();

    let row_vec: Vec<_> = rec_it
        .zip(col_types)
        .map(|(s, typ)| value_from_str(s, typ))
        .collect::<anyhow::Result<Vec<_>>>()
        .context("reading csv value")
        .to_glue_err()?;

    Ok(Row(row_vec))
}

trait IntoMutResult<T, U> {
    fn into_mut_result(self, t: T) -> MutResult<T, U>;
}

impl<T, U> IntoMutResult<T, U> for Result<U, GlueError> {
    fn into_mut_result(self, t: T) -> MutResult<T, U> {
        match self {
            Ok(val) => Ok((t, val)),
            Err(err) => Err((t, err)),
        }
    }
}

trait ToGlueError {
    type Target;

    fn to_glue_err(self) -> GlueResult<Self::Target>;
}

impl<T> ToGlueError for anyhow::Result<T> {
    type Target = T;

    fn to_glue_err(self) -> GlueResult<Self::Target> {
        self.map_err(|err| GlueError::StorageMsg(err.to_string()))
    }
}

impl CsvStore {
    async fn insert_schema(&mut self, schema: &Schema) -> anyhow::Result<()> {
        let table_id = TableIdentifier::new(schema.table_name.clone(), self.data_dir.clone());
        let path: TablePath = table_id.try_into()?;
        let headers = schema.column_defs.iter().map(|col| col.name.clone());
        let csv_path = path.as_csv();
        if let Some(parent) = csv_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut writer = csv::Writer::from_path(csv_path)?;

        writer.write_record(headers)?;

        Ok(())
    }

    async fn delete_schema(&mut self, table_name: &str) -> anyhow::Result<()> {
        println!("delete_data");
        dbg!(table_name);

        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id.try_into()?;
        std::fs::remove_file(path.as_csv())?;

        Ok(())
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<Row>) -> anyhow::Result<()> {
        println!("append_data");
        dbg!(table_name);
        dbg!(&rows);

        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id.try_into()?;
        let file = OpenOptions::new().append(true).open(path.as_csv())?;
        let mut writer = csv::WriterBuilder::new().from_writer(file);

        for row in rows {
            let values = row.0.into_iter().map(format_value);
            writer.write_record(values)?;
        }

        Ok(())
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Row)>) -> anyhow::Result<()> {
        println!("insert_data");
        dbg!(table_name);
        dbg!(&rows);

        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id.try_into()?;

        let mut numbered_rows: Vec<_> = rows
            .into_iter()
            .map(|(key, row)| get_row_num(&key).map(|row_num| (row_num, row)))
            .collect::<anyhow::Result<_>>()?;

        // Sort rows
        numbered_rows.sort_by_key(|(row_num, _row)| *row_num);

        // Extract sorted row_nums & data
        let mut row_nums = Vec::new();
        let mut row_data = Vec::new();
        for (row_num, row) in numbered_rows {
            // Add one to line numbers to account for headers
            row_nums.push(row_num + 1);
            row_data.push(row);
        }

        // Write new CSV rows to temporary buffer
        let mut buf = Vec::new();

        {
            let mut writer = csv::WriterBuilder::new().from_writer(&mut buf);

            // Write rows to temporary buffer
            for row in row_data {
                let values = row.0.into_iter().map(format_value);
                writer.write_record(values)?;
            }
        }

        let new_lines = buf.lines();
        let numbered_lines: Vec<_> = new_lines
            .zip(row_nums)
            .map(|(line, row_num)| line.map(|l| (row_num, l)))
            .collect::<std::io::Result<_>>()?;

        let previous_file = File::open(path.clone().as_csv())?;
        let previous_reader = BufReader::new(previous_file);

        let previous_lines = previous_reader.lines();

        let injection = Injection::new(numbered_lines);
        let injector = LineInjector::new(previous_lines, injection);

        // Write combined stream to buffer
        let mut buf = Vec::new();
        for line_res in injector {
            let combined_line = line_res?;
            writeln!(buf, "{}", combined_line)?;
        }

        // Overwrite original file with combined buffer
        let mut combined_file = File::create(path.as_csv())?;
        combined_file.write_all(&buf)?;

        Ok(())
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> anyhow::Result<()> {
        println!("delete_data");
        dbg!(table_name);
        dbg!(&keys);

        let table_id = TableIdentifier::new(table_name.to_string(), self.data_dir.clone());
        let path: TablePath = table_id.try_into()?;

        let mut delete_row_nums: Vec<_> = keys
            .iter()
            .map(get_row_num)
            .collect::<anyhow::Result<_>>()?;

        delete_row_nums.sort();
        delete_row_nums.reverse();

        let mut buf = Vec::new();

        let orig_file = BufReader::new(File::open(path.as_csv())?);

        for (line_num, line_res) in orig_file.lines().enumerate() {
            let line = line_res?;
            if let Some(&next_skip_line_num) = delete_row_nums.last() {
                if next_skip_line_num == line_num {
                    delete_row_nums.pop();
                }
            } else {
                writeln!(buf, "{}", line)?;
            }
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl StoreMut for CsvStore {
    async fn insert_schema(self, schema: &Schema) -> MutResult<Self, ()> {
        let mut storage = self;
        CsvStore::insert_schema(&mut storage, schema)
            .await
            .to_glue_err()
            .into_mut_result(storage)
    }

    async fn delete_schema(self, table_name: &str) -> MutResult<Self, ()> {
        let mut storage = self;
        CsvStore::delete_schema(&mut storage, table_name)
            .await
            .to_glue_err()
            .into_mut_result(storage)
    }

    async fn append_data(self, table_name: &str, rows: Vec<Row>) -> MutResult<Self, ()> {
        let mut storage = self;
        CsvStore::append_data(&mut storage, table_name, rows)
            .await
            .to_glue_err()
            .into_mut_result(storage)
    }

    async fn insert_data(self, table_name: &str, rows: Vec<(Key, Row)>) -> MutResult<Self, ()> {
        let mut storage = self;
        CsvStore::insert_data(&mut storage, table_name, rows)
            .await
            .to_glue_err()
            .into_mut_result(storage)
    }

    async fn delete_data(self, table_name: &str, keys: Vec<Key>) -> MutResult<Self, ()> {
        let mut storage = self;
        CsvStore::delete_data(&mut storage, table_name, keys)
            .await
            .to_glue_err()
            .into_mut_result(storage)
    }
}

impl GStore for CsvStore {}
impl GStoreMut for CsvStore {}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use gluesql::test_suite::*;

    use super::*;

    struct CsvTester {
        storage: Rc<RefCell<Option<CsvStore>>>,
    }

    impl Tester<CsvStore> for CsvTester {
        fn new(_: &str) -> Self {
            let tmpdir = tempdir::TempDir::new("csv-store-tester").expect("tmpdir");
            let config = Config {
                data_dir: tmpdir.path().to_str().expect("path conversion").to_owned(),
                ignores: vec![],
            };
            let storage = CsvStore::new(config);

            CsvTester {
                storage: Rc::new(RefCell::new(Some(storage))),
            }
        }

        fn get_cell(&mut self) -> Rc<RefCell<Option<CsvStore>>> {
            Rc::clone(&self.storage)
        }
    }

    generate_store_tests!(tokio::test, CsvTester);
}
