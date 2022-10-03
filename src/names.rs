use std::{convert::TryFrom, ops::Deref, path::PathBuf};

use anyhow::bail;

/// Path of the corresponding file (w/ extension)
#[derive(Debug, Clone)]
pub struct TablePath {
    path: PathBuf,
    root: PathBuf,
}

/// As used in SQL queries
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    name: String,
    root: PathBuf,
}

/// Hierarchical (e.g. slash-delimited) path/name system
#[derive(Debug, Clone)]
pub struct TableName {
    parts: Vec<String>,
    root: PathBuf,
}

// Name <-> Path

impl TryFrom<TableName> for TablePath {
    type Error = anyhow::Error;

    fn try_from(name: TableName) -> Result<Self, Self::Error> {
        let mut path = name.root.clone();
        path.extend(name.parts);

        let table_path = Self::try_new(path, name.root)?;

        Ok(table_path)
    }
}

impl TryFrom<TablePath> for TableName {
    type Error = anyhow::Error;

    fn try_from(table_path: TablePath) -> Result<Self, Self::Error> {
        match table_path.path.strip_prefix(table_path.root.clone()) {
            Ok(rel) => {
                let comp_to_str = |comp| match comp {
                    std::path::Component::Normal(os_str) => {
                        if let Some(s) = os_str.to_str() {
                            Ok(s.to_owned())
                        } else {
                            bail!("Funny path name: {:?}", os_str);
                        }
                    }
                    _ => bail!("Unexpected path component"),
                };
                let parts: Vec<_> = rel
                    .components()
                    .into_iter()
                    .map(comp_to_str)
                    .collect::<anyhow::Result<_>>()?;

                let new = Self::new(parts, table_path.root);

                Ok(new)
            }
            Err(_) => bail!("path is not in data directory"),
        }
    }
}

// Identifier <-> Name

impl TryFrom<TableName> for TableIdentifier {
    type Error = anyhow::Error;

    fn try_from(name: TableName) -> Result<Self, Self::Error> {
        let id = name.parts.join("/");
        let new = Self::new(id, name.root);

        Ok(new)
    }
}

impl TryFrom<TableIdentifier> for TableName {
    type Error = anyhow::Error;

    fn try_from(id: TableIdentifier) -> Result<Self, Self::Error> {
        let parts = id.name.split("/").map(ToOwned::to_owned).collect();
        let new = Self::new(parts, id.root);

        Ok(new)
    }
}

// Identifier <-> Path

impl TryFrom<TableIdentifier> for TablePath {
    type Error = anyhow::Error;

    fn try_from(identifier: TableIdentifier) -> Result<Self, Self::Error> {
        let name: TableName = identifier.try_into()?;
        name.try_into()
    }
}

impl TryFrom<TablePath> for TableIdentifier {
    type Error = anyhow::Error;

    fn try_from(path: TablePath) -> Result<Self, Self::Error> {
        let name: TableName = path.try_into()?;
        name.try_into()
    }
}

impl Deref for TableIdentifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

impl TableName {
    pub fn new(parts: Vec<String>, root: PathBuf) -> Self {
        Self { parts, root }
    }

    /// Return the last component of the table name, if any.
    pub fn last(&self) -> Option<String> {
        self.parts.iter().last().cloned()
    }
}

impl TablePath {
    pub fn try_new(path: PathBuf, root: PathBuf) -> anyhow::Result<Self> {
        if let Some(ext) = path.extension() {
            if ext != "csv" {
                bail!("table path with non-csv extension");
            }
        }
        let path = path.with_extension(""); // drop .csv
        let new = Self { path, root };

        Ok(new)
    }

    pub fn as_csv(self) -> PathBuf {
        self.path.with_extension("csv")
    }

    pub fn as_dir(self) -> PathBuf {
        self.path
    }
}

impl TableIdentifier {
    pub fn new(name: String, root: PathBuf) -> Self {
        Self { name, root }
    }
}
