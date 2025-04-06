use bevy::prelude::*;
use bevy::reflect::TypeInfo;
use bevy_erm::prelude::*;
use rusqlite::types::*;
use rusqlite::ToSql;

pub struct ValueWrapper<'a> {
    reg_type: TypeInfo,
    getter: &'a dyn Reflect,
}

impl<'a> ValueWrapper<'a> {
    pub fn build<T: Reflect + TypePath + Struct>(
        value: &'a T,
        field_name: &str,
        registry: &AppTypeRegistry,
    ) -> Self {
        let ty = bevy::reflect::Type::of::<T>()
            .type_path_table()
            .short_path();
        let type_info = registry
            .read()
            .get_with_short_type_path(ty)
            .unwrap()
            .type_info();
        let field = value.field(field_name).unwrap().try_as_reflect().unwrap();

        ValueWrapper {
            reg_type: type_info.to_owned(),
            getter: field,
        }
    }
}

impl ToSql for ValueWrapper<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let ty = *self.getter.reflect_type_info().ty();

        // Unsigned Integer
        if ty == bevy::reflect::Type::of::<u8>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<u8>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<u16>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<u16>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<u32>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<u32>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<u64>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<u64>().unwrap() as i64,
            )));
        }

        // Integer
        if ty == bevy::reflect::Type::of::<i8>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<i8>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<i16>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<i16>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<i32>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<i32>().unwrap() as i64,
            )));
        }

        if ty == bevy::reflect::Type::of::<i64>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Integer(
                *self.getter.downcast_ref::<i64>().unwrap(),
            )));
        }

        // Float
        if ty == bevy::reflect::Type::of::<f32>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Real(
                *self.getter.downcast_ref::<f32>().unwrap() as f64,
            )));
        }

        if ty == bevy::reflect::Type::of::<f64>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Real(
                *self.getter.downcast_ref::<f64>().unwrap(),
            )));
        }

        // Text
        if ty == bevy::reflect::Type::of::<String>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Text(
                self.getter.downcast_ref::<String>().unwrap().to_string(),
            )));
        }

        if ty == bevy::reflect::Type::of::<&str>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Text(
                self.getter.downcast_ref::<&str>().unwrap().to_string(),
            )));
        }

        // Vectors
        if ty == bevy::reflect::Type::of::<Vec2>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<Vec2>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<Vec3>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<Vec3>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<Vec4>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<Vec4>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<UVec2>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<UVec2>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<UVec3>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<UVec3>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<UVec4>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<UVec4>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<IVec2>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<IVec2>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<IVec3>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<IVec3>().unwrap().into_blob(),
            )));
        }

        if ty == bevy::reflect::Type::of::<IVec4>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<IVec4>().unwrap().into_blob(),
            )));
        }

        // Quat
        if ty == bevy::reflect::Type::of::<Quat>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<Quat>().unwrap().into_blob(),
            )));
        }

        // SRGB
        if ty == bevy::reflect::Type::of::<Srgba>() {
            return rusqlite::Result::Ok(ToSqlOutput::Owned(Value::Blob(
                self.getter.downcast_ref::<Srgba>().unwrap().into_blob(),
            )));
        }

        panic!("Cannot convert type {:?}", self.reg_type.ty().ident());
    }
}

#[cfg(test)]
mod tests {
    use super::ValueWrapper;
    use bevy::prelude::*;
    use rusqlite::ToSql;

    fn prepare_app() -> App {
        let mut app = App::new();
        let registry = AppTypeRegistry::default();
        app.insert_resource(registry);
        app.register_type::<Player>();

        app
    }

    #[derive(Default, Reflect, Clone)]
    #[reflect(Default)]
    struct Player {
        id: u64,
        name: String,
    }

    fn new_player() -> Player {
        Player {
            id: 2,
            name: "Test".to_string(),
        }
    }

    #[test]
    fn test_c_tor() {
        let subject = Player {
            id: 1,
            name: "Test".to_string(),
        };
        let field = subject.field("id").unwrap().try_as_reflect().unwrap();

        assert_eq!(*field.downcast_ref::<u64>().unwrap(), subject.id);
    }

    fn update_get_value_test(registry: ResMut<AppTypeRegistry>) {
        let p = new_player();
        let id_wrapper = ValueWrapper::build::<Player>(&p, "id", &registry);
        let v = id_wrapper.to_sql().unwrap();
        match v {
            rusqlite::types::ToSqlOutput::Owned(value) => match value {
                rusqlite::types::Value::Integer(v) => {
                    assert_eq!(p.id as i64, v);
                }
                _ => todo!(),
            },
            _ => todo!(),
        }

        let name_wrapper = ValueWrapper::build::<Player>(&p, "name", &registry);
        let v = name_wrapper.to_sql().unwrap();
        match v {
            rusqlite::types::ToSqlOutput::Owned(value) => match value {
                rusqlite::types::Value::Text(v) => {
                    assert_eq!(p.name, v);
                }
                _ => todo!(),
            },
            _ => todo!(),
        }
    }

    #[test]
    fn test_wrapper_get_value() {
        let mut app = prepare_app();
        app.add_systems(Update, update_get_value_test);

        app.update();
    }
}
