use juniper::{graphql_object, Context};
use simple_db::{DbValue, PinnedDatabase, Row, Table};
use std::collections::HashSet;

#[derive(Clone, Copy, Debug)]
pub struct Query;

#[derive(Clone, Debug)]
pub struct TableW(pub Table);

#[derive(Clone, Debug)]
pub struct DatabaseW(pub PinnedDatabase);

#[derive(Clone, Debug)]
pub struct RowW(pub Row);

#[derive(Clone, Debug)]
pub struct DbValueW(pub (DbValue, usize));

#[graphql_object(context = DatabaseW)]
/// The root query object of the schema
impl Query {
    fn table(
        #[graphql(context)] database: &DatabaseW,
        #[graphql(description = "name of the table")] id: String,
    ) -> Option<&TableW> {
        let x = database.0.get_table(id).ok();
        unsafe { std::mem::transmute(x) }
    }
}

impl Context for DatabaseW {}

#[graphql_object]
impl TableW {
    pub fn id(&self) -> &str {
        self.0.get_name()
    }

    pub fn rows(&self, sorted_by: Option<i32>) -> Vec<RowW> {
        self.0
            .get_rows_sorted(sorted_by.map(|s| s as usize))
            .map(|r| RowW(r.clone()))
            .collect()
    }
}

#[graphql_object]
impl RowW {
    pub fn values(&self, columns: Option<Vec<i32>>) -> Vec<DbValueW> {
        let is_none = columns.is_none();
        let columns: HashSet<usize> = columns.unwrap_or_default().into_iter().map(|i| i as usize).collect();
        self.0
             .0
            .iter()
            .enumerate()
            .filter_map(|(i, v)| (is_none || columns.contains(&i)).then_some(DbValueW((v.clone(), i))))
            .collect()
    }
}

#[graphql_object]
impl DbValueW {
    pub fn value(&self) -> String {
        self.0 .0.to_string()
    }

    pub fn column(&self) -> i32 {
        self.0 .1 as i32
    }
}
