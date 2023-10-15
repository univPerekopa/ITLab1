use simple_db::*;
use std::ops::{Deref, DerefMut};

use druid::widget::{Button, Flex, Label, TextBox};
use druid::{AppLauncher, Data, Lens, PlatformError, Widget, WidgetExt, WindowDesc};

// Wrapper around PinnedDatabase
#[derive(Debug, Clone)]
struct WrappedDb(Box<PinnedDatabase>);

impl Data for WrappedDb {
    fn same(&self, _other: &Self) -> bool {
        false
    }
}

impl Deref for WrappedDb {
    type Target = PinnedDatabase;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for WrappedDb {
    fn deref_mut(&mut self) -> &mut PinnedDatabase {
        self.0.deref_mut()
    }
}

#[derive(Debug, Clone, Data, Lens)]
struct AppData {
    opened_db: Option<WrappedDb>,
    path: String,
    path_new: String,
    db_name: String,
    table_name: String,
    sort_by: String,
    table_name_to_create: String,
    table_name_to_remove: String,
    table_schema: String,
    row_data: String,
    row_index: String,
}

fn main() -> Result<(), PlatformError> {
    let main_window = WindowDesc::new(ui_builder()).window_size((1500.0f64, 500.0f64));
    let data = AppData {
        opened_db: None,
        path: String::new(),
        path_new: String::new(),
        db_name: String::new(),
        table_name: String::new(),
        sort_by: String::new(),
        table_name_to_create: String::new(),
        table_name_to_remove: String::new(),
        table_schema: String::new(),
        row_data: String::new(),
        row_index: String::new(),
    };
    AppLauncher::with_window(main_window)
        .log_to_console()
        .launch(data)
}

fn ui_builder() -> impl Widget<AppData> {
    let label: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        format!(
            "Opened DB: {}",
            data.opened_db
                .as_ref()
                .map(|db| db.get_name().to_string())
                .unwrap_or("none".to_string())
        )
    });

    let button_open = Button::new("open db")
        .on_click(|_ctx, data: &mut AppData, _env| {
            data.opened_db = Some(WrappedDb(Box::new(
                PinnedDatabase::load_from_disk(data.path.clone()).unwrap(),
            )))
        })
        .padding(10.0);
    let tb_open = TextBox::new()
        .with_placeholder("put db path")
        .lens(AppData::path);
    let row_open = Flex::row()
        .with_child(button_open)
        .with_child(tb_open)
        .padding(50.0);

    let button_create = Button::new("create db")
        .on_click(|_ctx, data: &mut AppData, _env| {
            data.opened_db = Some(WrappedDb(Box::new(
                PinnedDatabase::create(data.db_name.clone(), data.path_new.clone()).unwrap(),
            )))
        })
        .padding(10.0);
    let tb_create1 = TextBox::new()
        .with_placeholder("put db path")
        .lens(AppData::path_new)
        .padding(10.0);
    let tb_create2 = TextBox::new()
        .with_placeholder("put db name")
        .lens(AppData::db_name);
    let row_create = Flex::row()
        .with_child(button_create)
        .with_child(tb_create1)
        .with_child(tb_create2)
        .padding(10.0);

    let table_list_label: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        let tables = data
            .opened_db
            .as_ref()
            .map(|db| db.get_table_names())
            .unwrap_or_default();
        format!("Tables: {:?}", tables)
    });
    let tb_open_table = TextBox::new()
        .with_placeholder("put table name")
        .lens(AppData::table_name);

    let row_tables = Flex::row()
        .with_child(table_list_label)
        .with_child(tb_open_table);

    let table_schema: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        if let Some(table) = data
            .opened_db
            .as_ref()
            .and_then(|db| db.get_table(data.table_name.clone()).ok())
        {
            format!("{:?}", table.schema())
        } else {
            "".to_string()
        }
    });

    let table_data: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        let mut sorted_by = if let Ok(idx) = data.sort_by.parse::<usize>() {
            Some(idx)
        } else {
            None
        };

        if let Some(table) = data
            .opened_db
            .as_ref()
            .and_then(|db| db.get_table(data.table_name.clone()).ok())
        {
            if sorted_by.unwrap_or(0) >= table.schema().len() {
                sorted_by = None;
            }
            table
                .get_rows_sorted(sorted_by)
                .map(|row| format!("{row}"))
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            "".to_string()
        }
    });

    let tb_sort_by = TextBox::new()
        .with_placeholder("put column index to sort by")
        .lens(AppData::sort_by)
        .fix_size(200.0f64, 25.0f64);
    let tb_row = TextBox::new()
        .with_placeholder("put new row data")
        .lens(AppData::row_data)
        .fix_size(200.0f64, 25.0f64);
    let button_add_row = Button::new("add row")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Some(db) = &mut data.opened_db else {
                return;
            };
            let Ok(table) = db.get_table_mut(data.table_name.clone()) else {
                return;
            };
            let Ok(row) = serde_json::from_str(&data.row_data) else {
                return;
            };
            let _ = table.insert_row(row);
            let _ = db.save();
        })
        .padding(10.0);

    let table_misc = Flex::row()
        .with_child(tb_sort_by)
        .with_child(tb_row)
        .with_child(button_add_row);

    let tb_row_idx = TextBox::new()
        .with_placeholder("put row index")
        .lens(AppData::row_index)
        .fix_size(200.0f64, 25.0f64);
    let button_remove_row = Button::new("remove row")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Some(db) = &mut data.opened_db else {
                return;
            };
            let Ok(table) = db.get_table_mut(data.table_name.clone()) else {
                return;
            };
            let Ok(index) = data.row_index.parse::<usize>() else {
                return;
            };
            if index < table.rows().len() {
                let _ = table.remove_row(index);
                let _ = db.save();
            }
        })
        .padding(10.0);
    let table_row_remove = Flex::row()
        .with_child(tb_row_idx)
        .with_child(button_remove_row);

    let button_create_table = Button::new("create table")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Some(db) = &mut data.opened_db else {
                return;
            };
            let Ok(schema) = serde_json::from_str(&data.table_schema) else {
                return;
            };
            let _ = db.create_table(data.table_name_to_create.clone(), schema);
            let _ = db.save();
        })
        .padding(10.0);
    let tb_create1 = TextBox::new()
        .with_placeholder("put table name")
        .lens(AppData::table_name_to_create)
        .padding(10.0);
    let tb_create2 = TextBox::new()
        .with_placeholder("put table schema")
        .lens(AppData::table_schema);
    let row_create_table = Flex::row()
        .with_child(button_create_table)
        .with_child(tb_create1)
        .with_child(tb_create2)
        .padding(10.0);

    let button_remove_table = Button::new("remove table")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Some(db) = &mut data.opened_db else {
                return;
            };

            let _ = db.remove_table(data.table_name_to_create.clone());
            let _ = db.save();
        })
        .padding(10.0);
    let tb_remove = TextBox::new()
        .with_placeholder("put table name")
        .lens(AppData::table_name_to_remove)
        .padding(10.0);
    let row_remove_table = Flex::row()
        .with_child(button_remove_table)
        .with_child(tb_remove)
        .padding(10.0);

    Flex::column()
        .with_child(label)
        .with_child(row_open)
        .with_child(row_create)
        .with_child(row_tables)
        .with_child(table_schema)
        .with_child(table_data)
        .with_child(table_misc)
        .with_child(table_row_remove)
        .with_child(row_create_table)
        .with_child(row_remove_table)
}
