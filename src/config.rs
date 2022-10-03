use serde::{Deserialize, Serialize};

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "~/feet".into(),
            ignores: vec![".git".to_string()],
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    // TODO: data_dir should be a shell-expanded and canonicalized PathBuf
    // via a custom parser function
    /// Data directory for CSV storage
    pub data_dir: String,

    /// File patterns to ignore when listing files/directories.
    /// Interpreted by globset.
    pub ignores: Vec<String>,
}
