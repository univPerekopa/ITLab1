use crate::{DbType, Row};

#[tarpc::service]
pub trait Service {
    async fn create(name: String, path: String);
    async fn open(path: String);
    async fn get_name() -> Option<String>;
    async fn get_table_names() -> Option<Vec<String>>;
    async fn save();
    async fn remove_table(name: String);
    async fn create_table(name: String, schema: Vec<DbType>);
    async fn remove_row(table: String, index: usize);
    async fn insert_row(table: String, row: Row);
    async fn get_table_schema(table: String) -> Option<Vec<DbType>>;
    async fn get_rows_sorted(table: String, sorted_by: Option<usize>) -> Option<Vec<Row>>;
}
