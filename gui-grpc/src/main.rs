use std::net::IpAddr;
use std::net::Ipv6Addr;

use std::ops::Deref;
use std::ops::DerefMut;

use std::time::Duration;

use druid::AppLauncher;
use druid::Data;
use druid::Lens;
use druid::PlatformError;
use druid::WindowDesc;
use druid::WidgetExt;
use druid::Widget;

use druid::widget::Button;
use druid::widget::Flex;
use druid::widget::Label;
use druid::widget::TextBox;

use simple_db::DbType;
use simple_db::grpc::database::CreateRequest;
use simple_db::grpc::database::CreateTableRequest;
use simple_db::grpc::database::EmptyRequest;
use simple_db::grpc::database::Name;
use simple_db::grpc::database::Path;
use simple_db::grpc::database::RowPosition;
use simple_db::grpc::database::Schema;
use simple_db::grpc::database::Table;
use tarpc::tokio_serde::formats::Json;
use tarpc::client;
use tarpc::context;
use tokio::runtime::Handle;

use simple_db::PinnedDatabase;
use simple_db::grpc::database;
use simple_db::grpc::database::service_client::ServiceClient;
use tonic::Request;
use tonic::transport::Channel;

// Wrapper around PinnedDatabase
#[derive(Debug, Clone)]
struct WrappedDb(Box<PinnedDatabase>);

// Wrapper around ServiceClient
#[derive(Debug, Clone)]
struct WrappedClient(ServiceClient<Channel>);

impl Data for WrappedDb {
    fn same(&self, _other: &Self) -> bool {
        true
    }
}

impl Data for WrappedClient {
    fn same(&self, _other: &Self) -> bool {
        true
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

impl Deref for WrappedClient {
    type Target = ServiceClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WrappedClient {
    fn deref_mut(&mut self) -> &mut ServiceClient<Channel> {
        &mut self.0
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
    client: WrappedClient,
    counter: usize,
}

#[tokio::main]
async fn main() -> Result<(), PlatformError> {
    let client = ServiceClient::connect("http://[::1]:50051").await.unwrap();

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
        client: WrappedClient(client),
        counter: 0,
    };
    AppLauncher::with_window(main_window)
        .log_to_console()
        .launch(data)
}

fn ui_builder() -> impl Widget<AppData> {
    let label: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        let r = Handle::current();
        let mut c = data.client.clone();
        let x = std::thread::spawn(move || r.block_on(c.get_name(EmptyRequest{})))
            .join()
            .unwrap();
        match x {
            Ok(name) => name.get_ref().name.clone(),
            Err(_) => "None".to_string(),
        }
    });

    let button_open = Button::new("open db")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let r = Handle::current();
            let mut c = data.client.clone();
            let p = data.path.clone();
            let _ = std::thread::spawn(move || r.block_on(c.open(Request::new(Path{path: p}))))
                .join()
                .unwrap();
            data.counter += 1;
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
            let r = Handle::current();
            let mut c = data.client.clone();
            let n = data.db_name.clone();
            let p = data.path_new.clone();
            let _ = std::thread::spawn(move || r.block_on(c.create(Request::new(CreateRequest{ name: n, path: p }))))
                .join()
                .unwrap();
            data.counter += 1;
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
        let r = Handle::current();
        let mut c = data.client.clone();
        let x = std::thread::spawn(move || r.block_on(c.get_table_names(EmptyRequest{})))
            .join()
            .unwrap();
        let tables = match x {
            Ok(names) => names.get_ref().names.iter().map(|n| n.name.clone()).collect(),
            Err(_) => vec!["None".to_string()],
        };
        format!("Tables: {:?}", tables)
    });
    let tb_open_table = TextBox::new()
        .with_placeholder("put table name")
        .lens(AppData::table_name);
    let row_tables = Flex::row()
        .with_child(table_list_label)
        .with_child(tb_open_table);

    let table_schema: Label<AppData> = Label::dynamic(|data: &AppData, _| {
        let r = Handle::current();
        let mut c = data.client.clone();
        let n = data.table_name.clone();
        let x = std::thread::spawn(move || r.block_on(c.get_table_schema(Request::new(Table{table: n}))))
            .join()
            .unwrap();
        let schema: Vec<DbType> = match x {
            Ok(schema) => {
                schema.get_ref().db_type.clone().iter().map(|v| match v.clone() {
                    0 => DbType::Int,
                    1 => DbType::Real,
                    2 => DbType::Char,
                    3 => DbType::String,
                    4 => DbType::ComplexReal,
                    5 => DbType::ComplexInt,
                    _ => panic!()
                }).collect()
            },
            Err(_) => todo!(),
        };
        format!("{:?}", schema)
    });

    // let table_data: Label<AppData> = Label::dynamic(|data: &AppData, _| {
    //     let sorted_by = if let Ok(idx) = data.sort_by.parse::<usize>() {
    //         Some(idx)
    //     } else {
    //         None
    //     };
    //     let r = Handle::current();
    //     let c = data.client.clone();
    //     let n = data.table_name.clone();
    //     if n.is_empty() {
    //         return String::new();
    //     }

    //     let x = std::thread::spawn(move || {
    //         r.block_on(c.get_rows_sorted(context::current(), n, sorted_by))
    //     })
    //     .join()
    //     .unwrap();

    //     let rows = x.unwrap_or(None).unwrap_or_default();
    //     rows.into_iter()
    //         .map(|row| format!("{row}"))
    //         .collect::<Vec<_>>()
    //         .join("\n")
    // });

    // let tb_sort_by = TextBox::new()
    //     .with_placeholder("put column index to sort by")
    //     .lens(AppData::sort_by)
    //     .fix_size(200.0f64, 25.0f64);
    // let tb_row = TextBox::new()
    //     .with_placeholder("put new row data")
    //     .lens(AppData::row_data)
    //     .fix_size(200.0f64, 25.0f64);
    // let button_add_row = Button::new("add row")
    //     .on_click(|_ctx, data: &mut AppData, _env| {
    //         let Ok(row) = serde_json::from_str(&data.row_data) else {
    //             return;
    //         };
    //         let r = Handle::current();
    //         let c = data.client.clone();
    //         let n = data.table_name.clone();
    //         std::thread::spawn(move || {
    //             r.block_on(c.insert_row(context::current(), n, row));
    //             r.block_on(c.save(context::current()));
    //         })
    //         .join()
    //         .unwrap();
    //         data.counter += 1;
    //     })
    //     .padding(10.0);

    // let table_misc = Flex::row()
    //     .with_child(tb_sort_by)
    //     .with_child(tb_row)
    //     .with_child(button_add_row);

    let tb_row_idx = TextBox::new()
        .with_placeholder("put row index")
        .lens(AppData::row_index)
        .fix_size(200.0f64, 25.0f64);
    let button_remove_row = Button::new("remove row")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Ok(index) = data.row_index.parse::<usize>() else {
                return;
            };
            let r = Handle::current();
            let mut c = data.client.clone();
            let n = data.table_name.clone();
            std::thread::spawn(move || {
                let _ = r.block_on(c.remove_row(Request::new(RowPosition{ table: n, index: index as u32 })));
                let _ = r.block_on(c.save(Request::new(EmptyRequest{})));
            })
            .join()
            .unwrap();
            data.counter += 1;
        })
        .padding(10.0);
    let table_row_remove = Flex::row()
        .with_child(tb_row_idx)
        .with_child(button_remove_row);

    let button_create_table = Button::new("create table")
        .on_click(|_ctx, data: &mut AppData, _env| {
            let Ok(schema): Result<Vec<DbType>, serde_json::Error> = serde_json::from_str(&data.table_schema) else {
                return;
            };

            let s = Schema {
                db_type: 
                    schema.iter().map(|t| match t {
                        DbType::Int => 0,
                        DbType::Real => 1,
                        DbType::Char => 2,
                        DbType::String => 3,
                        DbType::ComplexReal => 4,
                        DbType::ComplexInt => 5,
                    }).collect()
            };

            let r = Handle::current();
            let mut c = data.client.clone();
            let n = data.table_name_to_create.clone();
            std::thread::spawn(move || {
                let _ = r.block_on(c.create_table(Request::new(CreateTableRequest{ name: n, schema: Some(s) })));
                let _ = r.block_on(c.save(Request::new(EmptyRequest{})));
            })
            .join()
            .unwrap();
            data.counter += 1;
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
            let r = Handle::current();
            let mut c = data.client.clone();
            let n = data.table_name_to_remove.clone();
            std::thread::spawn(move || {
                let _ = r.block_on(c.remove_table(Request::new(Name{name: n})));
                let _ = r.block_on(c.save(Request::new(EmptyRequest{})));
            })
            .join()
            .unwrap();
            data.counter += 1;
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
        // .with_child(table_data)
        // .with_child(table_misc)
        .with_child(table_row_remove)
        .with_child(row_create_table)
        .with_child(row_remove_table)
}
