use std::clone;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::sync::Mutex;

use itertools::Itertools;
use simple_db::DbType;
use simple_db::PinnedDatabase;

use simple_db::grpc::database::CreateRequest;
use simple_db::grpc::database::CreateTableRequest;
use simple_db::grpc::database::EmptyRequest;
use simple_db::grpc::database::InsertRowRequest;
use simple_db::grpc::database::Name;
use simple_db::grpc::database::Names;
use simple_db::grpc::database::Path;
use simple_db::grpc::database::RowPosition;
use simple_db::grpc::database::Row;
use simple_db::grpc::database::Rows;
use simple_db::grpc::database::Schema;
use simple_db::grpc::database::SortedRequest;
use simple_db::grpc::database::SuccessfulResponse;
use simple_db::grpc::database::Table;
use simple_db::grpc::database::service_server::Service;
use simple_db::grpc::database::service_server::ServiceServer;

#[derive(Clone, Default)]
struct Server(pub Arc<Mutex<Option<PinnedDatabase>>>);

#[tonic::async_trait]
impl Service for Server {
    async fn create(&self, request: tonic::Request<CreateRequest>) 
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut lock = self.0.lock().unwrap();
        let new_db = PinnedDatabase::create(req.name, req.path).unwrap();
        lock.replace(new_db);

        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn open(&self, request: tonic::Request<Path>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut lock = self.0.lock().unwrap();
        let new_db = PinnedDatabase::load_from_disk(req.path).unwrap();
        lock.replace(new_db);

        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn get_name(&self, request: tonic::Request<EmptyRequest>)
        -> std::result::Result<tonic::Response<Name>, tonic::Status> {
        let req = request.into_inner();

        let lock = self.0.lock().unwrap();
        let name = lock.as_ref().map(|db| db.get_name().to_string());
        
        let reply = Name { 
            name: name.unwrap_or("None".to_string())
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn get_table_names(&self, request: tonic::Request<EmptyRequest>) 
        -> std::result::Result<tonic::Response<Names>, tonic::Status> {
        let req = request.into_inner();

        let lock = self.0.lock().unwrap();
        let names = lock.as_ref().map(|db| db.get_table_names());

        match names {
            Some(names) => {
                let reply = Names { 
                    names: names.iter().map(|n| Name{name: n.clone()}).collect_vec()
                };
                return Ok(tonic::Response::new(reply));
            },
            None => Ok(tonic::Response::new(Names { names: vec![ Name{ name: "None".to_string() } ] })),
        }
    }
    async fn save(&self, request: tonic::Request<EmptyRequest>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            db.save().unwrap();
        }
        
        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn remove_table(&self,request: tonic::Request<Name>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            db.remove_table(req.name).unwrap();
        }

        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn create_table(&self, request: tonic::Request<CreateTableRequest>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut schema= Vec::new();
        for db_type in req.schema.unwrap().db_type {
            schema.push(
                match db_type {
                    0 => DbType::Int,
                    1 => DbType::Real,
                    2 => DbType::Char,
                    3 => DbType::String,
                    4 => DbType::ComplexReal,
                    5 => DbType::ComplexInt,
                    _ => panic!()
                }
            )
        }

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            db.create_table(req.name, schema).unwrap();
        }
        
        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn remove_row(&self, request: tonic::Request<RowPosition>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(req.table) {
                table.remove_row(req.index as usize);
            }
        }
        
        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    //TODO: Implement parser or so
    async fn insert_row(&self, request: tonic::Request<InsertRowRequest>)
        -> std::result::Result<tonic::Response<SuccessfulResponse>, tonic::Status> {
        let req = request.into_inner();

        let db_row = Vec::new();
        for db_row_element in req.row.unwrap().value {
            todo!()
        }
        let row = simple_db::Row {
            0: db_row
        };

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table_mut(req.table) {
                let _ = table.insert_row(row);
            }
        }

        let reply = SuccessfulResponse {
            is_successful: true,
            err: "".to_string()
        };
        return Ok(tonic::Response::new(reply));
    }
    async fn get_table_schema(&self, request: tonic::Request<Table>)
        -> std::result::Result<tonic::Response<Schema>, tonic::Status> {
        let req = request.into_inner();

        let mut schema = Vec::new();

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(req.table) {
                for element in table.schema().to_vec() {
                    schema.push(
                        match element {
                            DbType::Int => 0,
                            DbType::Real => 1,
                            DbType::Char => 2,
                            DbType::String => 3,
                            DbType::ComplexReal => 4,
                            DbType::ComplexInt => 5,
                        }
                    )
                }
            }
        }

        let reply = Schema {
            db_type: schema
        };
        return Ok(tonic::Response::new(reply));
    }
    //TODO: Implement parser or so
    async fn get_rows_sorted(&self, request: tonic::Request<SortedRequest>)
        -> std::result::Result<tonic::Response<Rows>, tonic::Status> {
        let req = request.into_inner();

        let rows = Vec::new();

        let mut lock = self.0.lock().unwrap();
        if let Some(db) = lock.as_mut() {
            if let Ok(table) = db.get_table(req.table) {
                let rows_result: Vec<simple_db::Row> = table.get_rows_sorted(Some(req.sorted_by as usize)).cloned().collect();
                todo!()
            }
        };

        let reply = Rows {
            rows
        };
        return Ok(tonic::Response::new(reply));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = "[::1]:50051".parse().unwrap();
    let db = Arc::new(Mutex::new(None));
    Arc::new(Mutex::new(
        PinnedDatabase::load_from_disk("/home/tr3tiakoff/database/db".to_string()).unwrap(),
    ));
    let server = Server(db);

    tonic::transport::Server::builder()
        .add_service(ServiceServer::new(server))
        .serve(server_addr)
        .await.unwrap();

    Ok(())
}
