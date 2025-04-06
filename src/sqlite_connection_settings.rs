use bevy::prelude::*;
use std::fmt::Display;

#[derive(Resource, Clone)]
pub struct SqliteConnectionSettings {
    data_source: String,
    version: i32,
    utf_16_encoding: bool,
}

impl SqliteConnectionSettings {
    pub fn new() -> Self {
        SqliteConnectionSettings {
            data_source: "database.sqlite".to_owned(),
            version: 3,
            utf_16_encoding: false,
        }
    }

    pub fn set_data_source(&mut self, data_source: &str) {
        self.data_source = data_source.to_owned();
    }

    pub fn get_data_source(&self) -> &str {
        &self.data_source
    }

    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }

    pub fn use_utf_16_encoding(&mut self, value: bool) {
        self.utf_16_encoding = value;
    }
}

impl Default for SqliteConnectionSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for SqliteConnectionSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = format!(
            "Data Source={};Version={};UseUTF16Encoding={};",
            self.data_source,
            self.version,
            if self.utf_16_encoding {
                "True"
            } else {
                "False"
            }
        );

        write!(f, "{}", r)
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteConnectionSettings;

    #[test]
    fn test_default_connection_string() {
        let cs = SqliteConnectionSettings::new();
        assert_eq!(cs.data_source, "database.sqlite");
        assert_eq!(cs.version, 3);
        assert!(!cs.utf_16_encoding);
    }

    #[test]
    fn test_setters() {
        let mut cs = SqliteConnectionSettings::new();
        cs.set_data_source("test.sqlite");
        cs.set_version(2);
        cs.use_utf_16_encoding(true);
        assert_eq!(cs.data_source, "test.sqlite");
        assert_eq!(cs.version, 2);
        assert!(cs.utf_16_encoding);
    }

    #[test]
    fn test_to_string() {
        let cs = SqliteConnectionSettings::new();
        assert_eq!(
            cs.to_string(),
            "Data Source=database.sqlite;Version=3;UseUTF16Encoding=False;"
        );
    }
}
