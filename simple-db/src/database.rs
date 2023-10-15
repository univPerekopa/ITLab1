use crate::{
    table::Table,
    types::{DbError, DbType},
};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::{Entry, HashMap};
use std::fs::{create_dir_all, read, File};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct PinnedDatabase {
    db: Database,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Database {
    name: String,
    tables: HashMap<String, Table>,
}

impl PinnedDatabase {
    pub fn create(name: String, path: String) -> Result<Self, DbError> {
        let db = Database {
            name,
            tables: HashMap::new(),
        };
        let pinned_db = PinnedDatabase { db, path };
        pinned_db.save()?;

        Ok(pinned_db)
    }

    pub fn save(&self) -> Result<(), DbError> {
        let path = Path::new(&self.path);
        if let Some(prefix) = path.parent() {
            create_dir_all(prefix).unwrap();
        }
        let mut file = File::create(path)?;
        let content = bincode::serialize(&self.db)?;
        file.write_all(&content)?;

        Ok(())
    }

    pub fn load_from_disk(path: String) -> Result<Self, DbError> {
        let content = read(&path)?;
        let db: Database = bincode::deserialize(&content)?;
        for table in db.tables.values() {
            table.validate_rows()?;
        }

        Ok(PinnedDatabase { db, path })
    }

    pub fn create_table(&mut self, name: String, schema: Vec<DbType>) -> Result<(), DbError> {
        match self.db.tables.entry(name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(Table::new(name.clone(), schema));
                Ok(())
            }
            Entry::Occupied(_) => return Err(DbError::TableIsAlreadyPresent(name)),
        }
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.db.tables.keys().cloned().collect()
    }

    pub fn get_table_mut(&mut self, name: String) -> Result<&mut Table, DbError> {
        self.db
            .tables
            .get_mut(&name)
            .ok_or(DbError::TableIsMissing(name))
    }

    pub fn get_table(&self, name: String) -> Result<&Table, DbError> {
        self.db
            .tables
            .get(&name)
            .ok_or(DbError::TableIsMissing(name))
    }

    pub fn remove_table(&mut self, name: String) -> Result<(), DbError> {
        match self.db.tables.entry(name.clone()) {
            Entry::Occupied(entry) => {
                entry.remove();
                Ok(())
            }
            Entry::Vacant(_) => return Err(DbError::TableIsMissing(name)),
        }
    }

    pub fn get_name(&self) -> &str {
        self.db.name.as_str()
    }
}
