use crate::prelude::{SqliteConnectionSettings, ValueWrapper};
use bevy::{ prelude::*, reflect::{DynamicStruct, Type} };
use bevy_erm::prelude::{BevyERMPlugin, ColumnDefinition, FromBlob, TableDefinition};
use rusqlite::{types::FromSql, Connection, OptionalExtension, ToSql};
use std::sync::Mutex;

/// The database serves as a wrapper around the sqlite connection so we can use it as a resource.
#[derive(Default, Resource)]
pub struct SqliteDatabase {
    connection: Mutex<Option<Connection>>,
}

impl SqliteDatabase {
    /// Open the database file. The connection is stored guarded by a mutex.
    pub fn open(&mut self, connection_string: &SqliteConnectionSettings) -> Result<(), String> {
        if let Ok(mut c) = self.connection.lock() {
            let Ok(con) = Connection::open(connection_string.get_data_source()) else {
                return Err("Could not open database connection".to_owned());
            };

            *c = Some(con);
        }

        Ok(())
    }

    /// Close the database connection. This will set the connection to None.
    pub fn close(&mut self) -> Result<(), String> {
        match self.connection.lock() {
            Ok(mut c) => {
                let Some(con) = c.take() else {
                    return Ok(());
                };

                match con.close() {
                    Ok(_) => Ok(()),
                    Err(_) => Err("Could not close database connection.".to_string()),
                }
            }
            Err(_) => todo!(),
        }
    }

    /// Execute a query against the database. Returns the number of updated rows.
    pub fn execute(&mut self, query: &str, parameter: &[&dyn ToSql]) -> Result<usize, String> {
        match self.connection.lock() {
            Ok(c) => match c.as_ref() {
                Some(connection) => {
                    let mut r = connection.prepare(query).unwrap();
                    match r.execute(parameter) {
                        Ok(s) => Ok(s),
                        Err(e) => Err(format!("{}", e)),
                    }
                }
                None => todo!(),
            },
            Err(e) => Err(format!("{}", e)),
        }
    }

    /// Retrieve a single value from the database.
    pub fn query_scalar<T: Reflect + FromSql>(
        &mut self,
        query: &str,
        parameter: &[&dyn ToSql],
    ) -> Result<Option<T>, rusqlite::Error> {
        match self.connection.lock() {
            Ok(c) => match c.as_ref() {
                Some(connection) => match connection.prepare(query) {
                    Ok(mut stmt) => stmt
                        .query_row(parameter, |x| x.get::<usize, T>(0))
                        .optional(),
                    Err(e) => Err(e),
                },
                None => todo!(),
            },
            Err(_) => todo!(),
        }
    }

    pub fn query<T: Default + Reflect>(
        &mut self,
        table_def: &TableDefinition,
        query: &str,
        parameter: &[&dyn ToSql],
    ) -> Result<Vec<T>, String> {
        match self.connection.lock() {
            Ok(c) => match c.as_ref() {
                Some(connection) => {
                    let Ok(mut r) = connection.prepare(query) else {
                        return Err("Could not compile query!".to_string());
                    };

                    let names: Vec<String> =
                        r.column_names().iter().map(|x| x.to_string()).collect();

                    let result : Vec<T> = r.query_map(parameter, |row| {
                        // let mut value = table_def.reflect_default.default();
                        let mut value = T::default();
                        let mut dyn_type = DynamicStruct::default();

                        for (x, name) in names.iter().enumerate().clone() {
                            // let name = names[x].clone();
                            match table_def.get(name) {
                                Some(col) => match col.sql_type {
                                    bevy_erm::prelude::SqlType::None => panic!("Illegal SQL Type"),
                                    bevy_erm::prelude::SqlType::Integer(bits, not_null) => {
                                        match bits {
                                            8 => {
                                                let v = row.get_unwrap::<usize, i8>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            16 => {
                                                let v = row.get_unwrap::<usize, i16>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            32 => {
                                                let v = row.get_unwrap::<usize, i32>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            64 => {
                                                let v = row.get_unwrap::<usize, i64>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            _ => {
                                                panic!("Max bit size for integers is 64!")
                                            }
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::UnsingedInteger(bits, not_null) => {
                                        match bits {
                                            8 => {
                                                let v = row.get_unwrap::<usize, u8>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            16 => {
                                                let v = row.get_unwrap::<usize, u16>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            32 => {
                                                let v = row.get_unwrap::<usize, u32>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            64 => {
                                                let v = row.get_unwrap::<usize, u64>(x);
                                                if not_null {
                                                    dyn_type.insert(name, v);
                                                } else {
                                                    dyn_type.insert(name, Some(v));
                                                }
                                            }
                                            _ => {
                                                panic!("Max bit size for integers is 64!")
                                            }
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::Float(bits, not_null) => {
                                        if bits == 32 {
                                            let v = row.get_unwrap::<usize, f32>(x);
                                            if not_null {
                                                dyn_type.insert(name, v);
                                            } else {
                                                dyn_type.insert(name, Some(v));
                                            }
                                        } else if bits == 64 {
                                            let v = row.get_unwrap::<usize, f64>(x);
                                            if not_null {
                                                dyn_type.insert(name, v);
                                            } else {
                                                dyn_type.insert(name, Some(v));
                                            }
                                        } else {
                                            panic!("Floats must have 32 or 64 bits!")
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::Text(not_null) => {
                                        let v = row.get_unwrap::<usize, String>(x);
                                        if not_null {
                                            dyn_type.insert(name, v);
                                        } else {
                                            dyn_type.insert(name, Some(v));
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::Date(_) => todo!(),
                                    bevy_erm::prelude::SqlType::Time(_) => todo!(),
                                    bevy_erm::prelude::SqlType::DateTime(_) => todo!(),
                                    bevy_erm::prelude::SqlType::Blob(not_null) => {
                                        let v = row.get_unwrap::<usize, Vec<u8>>(x);
                                        // Vec2
                                        if col.ty.is::<Vec2>() && not_null {
                                            dyn_type.insert(name, Vec2::from_blob(&v));
                                        } else if col.ty.is::<Vec2>() && !not_null {
                                            dyn_type.insert(name, Some(Vec2::from_blob(&v)));
                                        }
                                        // Vec3
                                        else if col.ty.is::<Vec3>() && not_null {
                                            dyn_type.insert(name, Vec3::from_blob(&v));
                                        } else if col.ty.is::<Vec3>() && !not_null {
                                            dyn_type.insert(name, Some(Vec3::from_blob(&v)));
                                        }
                                        // Vec4
                                        else if col.ty.is::<Vec4>() && not_null {
                                            dyn_type.insert(name, Vec4::from_blob(&v));
                                        } else if col.ty.is::<Vec4>() && !not_null {
                                            dyn_type.insert(name, Some(Vec4::from_blob(&v)));
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::Boolean(not_null) => {
                                        let v = row.get_unwrap::<usize, bool>(x);
                                        if not_null {
                                            dyn_type.insert(name, v);
                                        } else {
                                            dyn_type.insert(name, Some(v));
                                        }
                                    }
                                    bevy_erm::prelude::SqlType::One2One(_type_id, _) => todo!(),
                                    bevy_erm::prelude::SqlType::Many2Many(_type_id, _) => todo!(),
                                },
                                None => {
                                    info!("Could not map column {}.", name);
                                }
                            }
                        }

                        value.apply(dyn_type.as_partial_reflect());

                        Ok(value)
                    }).unwrap().map(|x| x.unwrap()).collect();

                    Ok(result)
                }
                None => todo!(),
            },
            Err(e) => Err(format!("{}", e)),
        }
    }

    /// Returns true, if there is a table with the given name.
    pub fn table_exists(&mut self, table_name: &str) -> bool {
        let query = format!("SELECT Count(*) as Tables FROM sqlite_master WHERE type='table' AND name='{table_name}';");
        match self.query_scalar::<i32>(&query, &[]) {
            Ok(r) => r.unwrap() > 0,
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    // Get all columns of a table.
    // PRAGMA table_info('Player');

    pub fn get_table_sql(table: &TableDefinition) -> Result<String, String> {
        let mut columns: Vec<String> = Vec::new();
        let mut sorted : Vec<&ColumnDefinition> = table.fields.values().collect();
        sorted.sort_by(|a, b| a.order.cmp(&b.order));
        for def in sorted {
            let name = def.sql_name.clone();
            let mut column = name.clone();
            match def.sql_type {
                bevy_erm::prelude::SqlType::None => todo!(),
                bevy_erm::prelude::SqlType::Integer(_, not_null) => {
                    if def.is_key() {
                        column.push_str(" INTEGER PRIMARY KEY AUTOINCREMENT");
                    } else {
                        column.push_str(" INTEGER");
                        if not_null {
                            column.push_str(" NOT NULL");
                        }
                    }
                }
                bevy_erm::prelude::SqlType::UnsingedInteger(_, not_null) => {
                    column.push_str(" INTEGER");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                    column.push_str(&format!(" CHECK({column} >= 0)"));
                }
                bevy_erm::prelude::SqlType::Float(_, not_null) => {
                    column.push_str(" REAL");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::Text(not_null) => {
                    if def.has_max_length() {
                        column.push_str(&format!(" VARCHAR({})", def.get_max_length()));
                    } else {
                        column.push_str(" TEXT");
                    }
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::Date(not_null) => {
                    column.push_str(" TEXT");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::Time(not_null) => {
                    column.push_str(" REAL");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::DateTime(not_null) => {
                    column.push_str(" TEXT");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::Blob(not_null) => {
                    column.push_str(" BLOB");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                }
                bevy_erm::prelude::SqlType::Boolean(not_null) => {
                    column.push_str(" INTEGER");
                    if not_null {
                        column.push_str(" NOT NULL");
                    }
                    column.push_str(&format!(" CHECK({name} >= 0 AND {name} < 2)"));
                }
                bevy_erm::prelude::SqlType::One2One(_type_id, _) => todo!(),
                bevy_erm::prelude::SqlType::Many2Many(_type_id, _) => todo!(),
            }

            columns.push(column);
        }

        let table_name = table.sql_name.clone();
        let column_defs = columns.join(",\n");
        let sql = format!("CREATE TABLE '{table_name}'({column_defs});");

        Ok(sql)
    }

    /// Create a new table from the given table definition. If the table already exists,
    /// it will not be created. This method prints an info instead and returns ok.
    pub fn create_table(&mut self, def: &TableDefinition) -> Result<(), String> {
        let table_name = def.sql_name.clone();
        if self.table_exists(&table_name) {
            info!("A table with the name {table_name} already exists");
            return Ok(());
        }

        let Ok(table_sql) = Self::get_table_sql(def) else {
            return Err("Could not generate SQL command to create the table.".to_string());
        };

        match self.execute(&table_sql, &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn insert<T: Reflect + Default + TypePath + bevy::prelude::Struct>(
        &mut self,
        def: &TableDefinition,
        value: &T,
        registry: &AppTypeRegistry,
    ) -> Result<usize, String> {
        let table_name = def.sql_name.clone();
        assert_eq!(table_name, Type::of::<T>().short_path());

        let mut names_vec: Vec<String> = Vec::new();
        let mut params_vec: Vec<String> = Vec::new();
        let mut wrapped_values: Vec<ValueWrapper> = Vec::new();

        for x in def.fields.values() {
            if x.is_key() {
                continue;
            }

            names_vec.push(x.sql_name.clone());
            params_vec.push("?".to_owned());

            let wrapped_value = ValueWrapper::build(value, &x.rust_name, registry);
            wrapped_values.push(wrapped_value);
        }

        let column_names = names_vec.join(", ");
        let parameter = params_vec.join(", ");

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({});",
            table_name, column_names, parameter
        );

        let wrapped_links: Vec<&dyn ToSql> =
            wrapped_values.iter().map(|x| x as &dyn ToSql).collect();

        self.execute(&query, &wrapped_links)
    }
}

impl Plugin for SqliteDatabase {
    fn build(&self, app: &mut App) {
        app.add_plugins(BevyERMPlugin);

        app.insert_resource(SqliteConnectionSettings::default());
        app.insert_resource(SqliteDatabase::default());
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteDatabase;
    use crate::prelude::SqliteConnectionSettings;
    use bevy::prelude::*;
    use bevy_erm::prelude::{ErmTypesRegistry, Key, TableDefinition};

    #[derive(Default, Reflect)]
    #[reflect(Default)]
    struct Player {
        #[reflect(@Key)]
        id: i32,
        name: String,
        deaths: i32,
        email: String,
    }

    fn setup() -> App {
        let mut app = App::new();
        app.insert_resource(AppTypeRegistry::default());
        app.add_plugins(SqliteDatabase::default());
        app.register_type::<Player>();

        app
    }

    // Test 1
    fn update_database_path_1(mut settings: ResMut<SqliteConnectionSettings>) {
        settings.set_data_source("test_1.sqlite");
    }
    fn run_test_1(mut database: ResMut<SqliteDatabase>, settings: Res<SqliteConnectionSettings>) {
        database.open(&settings).unwrap();
        assert!(!database.table_exists("Player"));

        let rows = database
            .execute(
                "
            CREATE TABLE Player 
            (
                id INTEGER PRIMARY KEY, 
                name TEXT NOT NULL, 
                deaths INTEGER NOT NULL DEFAULT 0
            );",
                &[],
            )
            .unwrap();
        assert_eq!(rows, 0);
        assert!(database.table_exists("Player"));

        // Delete the file, so we can rerun the test
        std::fs::remove_file(settings.get_data_source()).unwrap();

        database.close().unwrap();
    }

    #[test]
    fn test_database_connection() {
        let mut app = setup();
        app.add_systems(PreStartup, update_database_path_1);
        app.add_systems(Startup, run_test_1);

        app.update();
    }

    // Test 2
    fn update_database_path_2(
        mut settings: ResMut<SqliteConnectionSettings>,
        app_registry: Res<AppTypeRegistry>,
        mut registry: ResMut<ErmTypesRegistry>,
    ) {
        settings.set_data_source("test_2.sqlite");
        registry.register_type::<Player>(&app_registry);
    }
    fn run_test_2(
        registry: Res<ErmTypesRegistry>,
        mut database: ResMut<SqliteDatabase>,
        settings: Res<SqliteConnectionSettings>,
    ) {
        database.open(&settings).unwrap();
        assert!(!database.table_exists("Player"));

        assert!(registry.get_table_definition("Player").is_some());
        assert!(database
            .create_table(registry.get_table_definition("Player").unwrap())
            .is_ok());

        assert!(database.table_exists("Player"));

        // Delete the file, so we can rerun the test
        std::fs::remove_file(settings.get_data_source()).unwrap();

        database.close().unwrap();
    }

    #[test]
    fn test_simple_table_generation() {
        let mut app = setup();
        app.add_systems(PreStartup, update_database_path_2);
        app.add_systems(Startup, run_test_2);

        app.update();
    }

    // Test 3
    fn update_database_path_3(
        mut settings: ResMut<SqliteConnectionSettings>,
        app_registry: Res<AppTypeRegistry>,
        mut registry: ResMut<ErmTypesRegistry>,
    ) {
        settings.set_data_source("test_3.sqlite");
        registry.register_type::<Player>(&app_registry);
    }

    fn insert_player(
        table : &TableDefinition, 
        registry : &AppTypeRegistry,
        database : &mut SqliteDatabase,
        deaths : i32, 
        name : &str,
        email : &str) {
        let test = Player {
            deaths,
            name: name.to_string(),
            email : email.to_string(),

            ..Default::default()
        };

        match database.insert(table, &test, registry) {
            Ok(_) => {}
            Err(e) => {
                println!("Could not insert item: {}", e);
                panic!("Could not insert item: {}", e);
            }
        }
    }

    fn run_test_3(
        registry: Res<AppTypeRegistry>,
        erm_registry: Res<ErmTypesRegistry>,
        mut database: ResMut<SqliteDatabase>,
        settings: Res<SqliteConnectionSettings>,
    ) {
        database.open(&settings).unwrap();

        assert!(erm_registry.get_table_definition("Player").is_some());
        assert!(database
            .create_table(erm_registry.get_table_definition("Player").unwrap())
            .is_ok());

        let table = erm_registry.get_table_definition("Player").unwrap();
        insert_player(table, &registry, &mut database, 10, "Runna vom Sofa", "test_1@testen.com");
        insert_player(table, &registry, &mut database, 30, "Anne Straße", "test_2@testen.com");
        insert_player(table, &registry, &mut database, 100, "Timo Beil", "test_3@testen.com");
        insert_player(table, &registry, &mut database, 24, "Rainer Szuvall", "test_4@testen.com");

        let test : Vec<Player> = database.query(table, "SELECT * FROM 'Player' WHERE name LIKE 'Timo%';", &[]).unwrap();
        assert!(!test.is_empty());
        assert_eq!(test[0].deaths, 100);
        assert_eq!(test[0].name, "Timo Beil".to_string());
        assert_eq!(test[0].email, "test_3@testen.com".to_string());

        // Delete the file, so we can rerun the test
        std::fs::remove_file(settings.get_data_source()).unwrap();

        database.close().unwrap();
    }

    #[test]
    fn test_insert_item() {
        let mut app = setup();
        app.add_systems(PreStartup, update_database_path_3);
        app.add_systems(Startup, run_test_3);

        app.update();
    }
}
