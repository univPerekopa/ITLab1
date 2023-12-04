use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tokio::sync::Mutex;

use simple_db::{DbType, PinnedDatabase, Row, Service};

#[derive(Clone)]
struct Server(pub Arc<Mutex<Option<PinnedDatabase>>>);

#[tarpc::server]
impl Service for Server {
    async fn create(self, _: tarpc::context::Context, name: String, path: String) {
        let mut lock = self.0.lock().await;
        let new_db = PinnedDatabase::create(name, path).unwrap();
        lock.replace(new_db);
    }

    async fn open(self, _: tarpc::context::Context, path: String) {
        let mut lock = self.0.lock().await;
        let new_db = PinnedDatabase::load_from_disk(path).unwrap();
        lock.replace(new_db);
    }

    async fn get_name(self, _: tarpc::context::Context) -> Option<String> {
        let lock = self.0.lock().await;
        lock.as_ref().map(|db| db.get_name().to_string())
    }

    async fn get_table_names(self, _: tarpc::context::Context) -> Option<Vec<String>> {
        let lock = self.0.lock().await;
        lock.as_ref().map(|db| db.get_table_names())
    }

    async fn save(self, _: tarpc::context::Context) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.save().unwrap();
        }
    }

    async fn remove_table(self, _: tarpc::context::Context, name: String) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.remove_table(name).unwrap();
        }
    }

    async fn create_table(self, _: tarpc::context::Context, name: String, schema: Vec<DbType>) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            db.create_table(name, schema).unwrap();
        }
    }

    async fn remove_row(self, _: tarpc::context::Context, table: String, index: usize) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(table) {
                table.remove_row(index);
            }
        }
    }

    async fn insert_row(self, _: tarpc::context::Context, table: String, row: Row) {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(table) {
                let _ = table.insert_row(row);
            }
        }
    }

    async fn get_table_schema(
        self,
        _: tarpc::context::Context,
        table: String,
    ) -> Option<Vec<DbType>> {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(table) {
                return Some(table.schema().to_vec());
            }
        }
        None
    }

    async fn get_rows_sorted(
        self,
        _: tarpc::context::Context,
        table: String,
        sorted_by: Option<usize>,
    ) -> Option<Vec<Row>> {
        let mut lock = self.0.lock().await;
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(table) {
                return Some(table.get_rows_sorted(sorted_by).cloned().collect());
            }
        }
        None
    }
}

#[tokio::main]
async fn main() {
    let server_addr = (IpAddr::V6(Ipv6Addr::LOCALHOST), 1337);
    let db = Arc::new(Mutex::new(None));
    Arc::new(Mutex::new(
        PinnedDatabase::load_from_disk("./db".to_string()).unwrap(),
    ));
    // JSON transport is provided by the json_transport tarpc module. It makes it easy
    // to start up a serde-powered json serialization strategy over TCP.
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default)
        .await
        .unwrap();
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let db = db.clone();
            let server = Server(db);
            channel.execute(server.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10000)
        .for_each(|_| async {})
        .await;
}