use std::path::PathBuf;

use serde::{Deserialize, Serialize};

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "~/feet".into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub data_dir: PathBuf,
}
