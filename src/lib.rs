mod plugin;
mod sqlite_connection_settings;
mod value_to_sql_wrapper;

pub mod prelude {
    pub use crate::plugin::SqliteDatabase;
    pub use crate::sqlite_connection_settings::SqliteConnectionSettings;
    pub use crate::value_to_sql_wrapper::ValueWrapper;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_database() {}
}
