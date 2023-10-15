use crate::*;
use tempfile::tempdir;

#[test]
fn load_db_from_disk() {
    // Create new database
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let db = PinnedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    // Drop DB
    drop(db);

    // Load the DB from disk
    let db = PinnedDatabase::load_from_disk(path.to_str().unwrap().to_string()).unwrap();
    assert_eq!(db.get_name(), "db");
}

#[test]
fn test_table_crud() {
    // Create new database
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let mut db =
        PinnedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    // Create new table
    db.create_table("table".to_string(), vec![DbType::Int])
        .unwrap();
    {
        let table = db.get_table_mut("table".to_string()).unwrap();

        table.insert_row(Row(vec![DbValue::Int(1)])).unwrap();
        assert_eq!(table.rows().len(), 1);

        table.update_row(0, Row(vec![DbValue::Int(2)])).unwrap();
        assert_eq!(table.rows().len(), 1);
        assert_eq!(table.rows()[0], Row(vec![DbValue::Int(2)]));

        table.remove_row(0);
        assert_eq!(table.rows().len(), 0);
    }

    db.remove_table("table".to_string()).unwrap();
    assert!(db.get_table_names().is_empty());
}

#[test]
fn get_sorted_table() {
    // Create new database
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let mut db =
        PinnedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    // Create new table
    db.create_table(
        "table".to_string(),
        vec![DbType::String, DbType::ComplexInt],
    )
    .unwrap();
    let table = db.get_table_mut("table".to_string()).unwrap();

    let row1 = Row(vec![
        DbValue::String("B".to_string()),
        DbValue::ComplexInt((1, 1)),
    ]);
    let row2 = Row(vec![
        DbValue::String("C".to_string()),
        DbValue::ComplexInt((0, 1)),
    ]);
    let row3 = Row(vec![
        DbValue::String("A".to_string()),
        DbValue::ComplexInt((0, -1)),
    ]);
    table.insert_row(row1.clone()).unwrap();
    table.insert_row(row2.clone()).unwrap();
    table.insert_row(row3.clone()).unwrap();

    // Check rows we can get rows in order of insertion
    let mut iter = table.get_rows_sorted(None);
    assert_eq!(iter.next().unwrap(), &row1);
    assert_eq!(iter.next().unwrap(), &row2);
    assert_eq!(iter.next().unwrap(), &row3);

    // Check rows we can get rows sorted by the first column
    let mut iter = table.get_rows_sorted(Some(0));
    assert_eq!(iter.next().unwrap(), &row3);
    assert_eq!(iter.next().unwrap(), &row1);
    assert_eq!(iter.next().unwrap(), &row2);

    // Check rows we can get rows sorted by the second column
    let mut iter = table.get_rows_sorted(Some(1));
    assert_eq!(iter.next().unwrap(), &row3);
    assert_eq!(iter.next().unwrap(), &row2);
    assert_eq!(iter.next().unwrap(), &row1);
}
