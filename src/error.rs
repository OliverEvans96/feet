use std::convert::{TryFrom, TryInto};

use anyhow::bail;
use gluesql::core::result::Error as GlueError;

use gluesql::core::{
    data::{
        IntervalError, KeyError, LiteralError, RowError, StringExtError, TableError, ValueError,
    },
    executor::{
        AggregateError, AlterError, EvaluateError, ExecuteError, FetchError, SelectError,
        UpdateError, ValidateError,
    },
    plan::PlanError,
    translate::TranslateError,
};
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum GlueErrorNoStorage {
    #[error("storage error: {0}")]
    StorageMsg(String),

    #[error("parsing failed: {0}")]
    Parser(String),

    #[error(transparent)]
    Translate(#[from] TranslateError),

    #[cfg(feature = "alter-table")]
    #[error(transparent)]
    AlterTable(#[from] AlterTableError),

    #[cfg(feature = "index")]
    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Execute(#[from] ExecuteError),
    #[error(transparent)]
    Alter(#[from] AlterError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Evaluate(#[from] EvaluateError),
    #[error(transparent)]
    Select(#[from] SelectError),
    #[error(transparent)]
    Aggregate(#[from] AggregateError),
    #[error(transparent)]
    Update(#[from] UpdateError),
    #[error(transparent)]
    Row(#[from] RowError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Validate(#[from] ValidateError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Value(#[from] ValueError),
    #[error(transparent)]
    Literal(#[from] LiteralError),
    #[error(transparent)]
    Interval(#[from] IntervalError),
    #[error(transparent)]
    StringExt(#[from] StringExtError),
    #[error(transparent)]
    Plan(#[from] PlanError),
}

impl TryFrom<GlueError> for GlueErrorNoStorage {
    type Error = anyhow::Error;

    fn try_from(value: GlueError) -> Result<Self, Self::Error> {
        let ret = match value {
            GlueError::Storage(_) => bail!("Cannot handle storage error"),
            GlueError::StorageMsg(inner) => Self::StorageMsg(inner),
            GlueError::Parser(inner) => Self::Parser(inner),
            GlueError::Translate(inner) => Self::Translate(inner),
            GlueError::Execute(inner) => Self::Execute(inner),
            GlueError::Alter(inner) => Self::Alter(inner),
            GlueError::Fetch(inner) => Self::Fetch(inner),
            GlueError::Evaluate(inner) => Self::Evaluate(inner),
            GlueError::Select(inner) => Self::Select(inner),
            GlueError::Aggregate(inner) => Self::Aggregate(inner),
            GlueError::Update(inner) => Self::Update(inner),
            GlueError::Row(inner) => Self::Row(inner),
            GlueError::Table(inner) => Self::Table(inner),
            GlueError::Validate(inner) => Self::Validate(inner),
            GlueError::Key(inner) => Self::Key(inner),
            GlueError::Value(inner) => Self::Value(inner),
            GlueError::Literal(inner) => Self::Literal(inner),
            GlueError::Interval(inner) => Self::Interval(inner),
            GlueError::StringExt(inner) => Self::StringExt(inner),
            GlueError::Plan(inner) => Self::Plan(inner),
        };

        Ok(ret)
    }
}

impl From<GlueErrorNoStorage> for GlueError {
    fn from(value: GlueErrorNoStorage) -> Self {
        match value {
            GlueErrorNoStorage::StorageMsg(inner) => Self::StorageMsg(inner),
            GlueErrorNoStorage::Parser(inner) => Self::Parser(inner),
            GlueErrorNoStorage::Translate(inner) => Self::Translate(inner),
            GlueErrorNoStorage::Execute(inner) => Self::Execute(inner),
            GlueErrorNoStorage::Alter(inner) => Self::Alter(inner),
            GlueErrorNoStorage::Fetch(inner) => Self::Fetch(inner),
            GlueErrorNoStorage::Evaluate(inner) => Self::Evaluate(inner),
            GlueErrorNoStorage::Select(inner) => Self::Select(inner),
            GlueErrorNoStorage::Aggregate(inner) => Self::Aggregate(inner),
            GlueErrorNoStorage::Update(inner) => Self::Update(inner),
            GlueErrorNoStorage::Row(inner) => Self::Row(inner),
            GlueErrorNoStorage::Table(inner) => Self::Table(inner),
            GlueErrorNoStorage::Validate(inner) => Self::Validate(inner),
            GlueErrorNoStorage::Key(inner) => Self::Key(inner),
            GlueErrorNoStorage::Value(inner) => Self::Value(inner),
            GlueErrorNoStorage::Literal(inner) => Self::Literal(inner),
            GlueErrorNoStorage::Interval(inner) => Self::Interval(inner),
            GlueErrorNoStorage::StringExt(inner) => Self::StringExt(inner),
            GlueErrorNoStorage::Plan(inner) => Self::Plan(inner),
        }
    }
}

pub trait Sendify<T> {
    fn sendify(self) -> anyhow::Result<Result<T, GlueErrorNoStorage>>;
}

impl<T> Sendify<T> for Result<T, GlueError> {
    fn sendify(self) -> anyhow::Result<Result<T, GlueErrorNoStorage>> {
        match self {
            Ok(t) => Ok(Ok(t)),
            Err(err) => Ok(Err(err.try_into()?)),
        }
    }
}
