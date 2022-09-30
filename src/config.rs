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
    // TODO: data_dir should be a shell-expanded and canonicalized PathBuf
    // via a custom parser function
    pub data_dir: String,
}
