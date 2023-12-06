use actix_web::http::StatusCode;
use actix_web::web::Data;
use actix_web::{web, HttpServer, App, Responder, HttpResponse};
use futures::{future, prelude::*};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tokio::sync::Mutex;

use simple_db::{DbType, PinnedDatabase, Row};

#[derive(Serialize, Deserialize)]
struct CreateRequest {
    name: String,
    path: String,
}
async fn create(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<CreateRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    let new_db = PinnedDatabase::create(request.name.clone(), request.path.clone()).unwrap();
    lock.replace(new_db);
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct OpenRequest {
    path: String,
}
async fn open(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<OpenRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    let new_db = PinnedDatabase::load_from_disk(request.path.clone()).unwrap();
    lock.replace(new_db);
    HttpResponse::Ok()
}

async fn get_name(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>) -> impl Responder {
    let lock = database.lock().await;
    let string = lock.as_ref().map(|db| db.get_name().to_string());
    HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&string).expect("Failed to serialize"))
}

async fn get_table_names(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>) -> impl Responder {
    let lock = database.lock().await;
    let strings = lock.as_ref().map(|db| db.get_table_names());
    HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&strings).expect("Failed to serialize"))
}

async fn save(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>) -> impl Responder {
    let mut lock = database.lock().await;
    if let Some(db) = lock.as_mut() {
        db.save().unwrap();
    }
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct RemoveTableRequest {
    name: String,
}
async fn remove_table(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<RemoveTableRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    if let Some(db) = lock.as_mut() {
        db.remove_table(request.name.clone()).unwrap();
    }
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct CreateTableRequest {
    name: String,
    schema: Vec<DbType>
}
async fn create_table(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<CreateTableRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    if let Some(db) = lock.as_mut() {
        db.create_table(request.name.clone(), request.schema.clone()).unwrap();
    }
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct RemoveRowRequest {
    table: String,
    index: usize,
}
async fn remove_row(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<RemoveRowRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    if let Some(db) = lock.as_mut() {
        if let Ok(table) = db.get_table_mut(request.table.clone()) {
            table.remove_row(request.index.clone());
        }
    }
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct InsertRowRequest {
    table: String,
    row: Row,
}
async fn insert_row(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<InsertRowRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    if let Some(db) = lock.as_mut() {
        if let Ok(table) = db.get_table_mut(request.table.clone()) {
            let _ = table.insert_row(request.row.clone());
        }
    }
    HttpResponse::Ok()
}

#[derive(Serialize, Deserialize)]
struct GetTableRequest {
    table: String,
}
async fn get_table_schema(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<GetTableRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    let mut table_result = None;
    if let Some(db) = lock.as_mut() {
        if let Ok(table) = db.get_table(request.table.clone()) {
            table_result = Some(table.schema().to_vec());
        }
    }
    HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&table_result).expect("Failed to serialize"))
}

#[derive(Serialize, Deserialize)]
struct GetRowsSortedRequest {
    table: String,
    sorted_by: Option<usize>,
}
async fn get_rows_sorted(database: web::Data<Arc<Mutex<Option<PinnedDatabase>>>>, request: web::Json<GetRowsSortedRequest>) -> impl Responder {
    let mut lock = database.lock().await;
    let mut row_result: Option<Vec<Row>> = None;
    if let Some(db) = lock.as_mut() {
        if let Ok(table) = db.get_table(request.table.clone()) {
            row_result = Some(table.get_rows_sorted(request.sorted_by.clone()).cloned().collect());
        }
    }
    HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&row_result).expect("Failed to serialize"))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    
    let db: Arc<Mutex<Option<PinnedDatabase>>> = Arc::new(Mutex::new(None));
    
    HttpServer::new(move || 
        App::new()
            .app_data(Data::new(db.clone()))
            .route("/create", web::post().to(create))
            .route("/open", web::post().to(open))
            .route("/get_name", web::get().to(get_name))
            .route("/get_table_names", web::get().to(get_table_names))
            .route("/save", web::post().to(save))
            .route("/remove_table", web::delete().to(remove_table))
            .route("/create_table", web::post().to(create_table))
            .route("/remove_row", web::delete().to(remove_row))
            .route("/insert_row", web::post().to(insert_row))
            .route("/get_table_schema", web::get().to(get_table_schema))
            .route("/get_rows_sorted", web::get().to(get_rows_sorted)))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}