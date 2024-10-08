use std::fmt::Write;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

// use tempfile::NamedTempFile;
use tempfile::NamedTempFile;

use crate::interpreter::btree::BtreeContext;
use crate::interpreter::pager::PageId;
use crate::interpreter::pager::Pager;
use crate::interpreter::payload::Payload;
use crate::interpreter::query::QueryPlan;
use crate::interpreter::record::RecordPayload;
use crate::interpreter::schema::Schema;
use crate::interpreter::value::Collation;
use crate::interpreter::value::Value;
use crate::interpreter::value::ValueCmp;
use crate::Connection;
use crate::DatabaseHeader;
use crate::Expression;
use crate::SelectStatement;
use crate::DATABASE_HEADER_SIZE;

pub fn create_sqlite_database(queries: &[&str]) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(file.path()).unwrap();
    for query in queries {
        conn.execute(query, []).unwrap();
    }
    conn.close().unwrap();
    file
}

pub fn create_pager(file: File) -> anyhow::Result<Pager> {
    let mut header_buf = [0_u8; DATABASE_HEADER_SIZE];
    file.read_exact_at(&mut header_buf, 0)?;
    let header = DatabaseHeader::from(&header_buf);
    Ok(Pager::new(
        file,
        header.n_pages(),
        header.pagesize(),
        header.pagesize() - header.reserved() as u32,
        header.first_freelist_trunk_page_id(),
        header.n_freelist_pages(),
    )?)
}

pub fn create_empty_pager(file_content: &[u8], pagesize: u32, usable_size: u32) -> Pager {
    let file = NamedTempFile::new().unwrap();
    file.as_file().write_all_at(file_content, 0).unwrap();
    Pager::new(
        file.as_file().try_clone().unwrap(),
        file_content.len() as u32 / pagesize,
        pagesize,
        usable_size,
        None,
        0,
    )
    .unwrap()
}

pub fn load_btree_context(file: &File) -> anyhow::Result<BtreeContext> {
    let mut header_buf = [0_u8; DATABASE_HEADER_SIZE];
    file.read_exact_at(&mut header_buf, 0)?;
    let header = DatabaseHeader::from(&header_buf);
    Ok(BtreeContext::new(
        header.pagesize() - header.reserved() as u32,
    ))
}

pub fn buffer_to_hex(buf: &[u8]) -> String {
    buf.iter().fold(String::new(), |mut output, v| {
        let _ = write!(output, "{v:02X}");
        output
    })
}

pub fn find_table_page_id(table: &str, filepath: &Path) -> PageId {
    let conn = Connection::open(filepath).unwrap();
    let schema_table = Schema::schema_table();
    let columns = schema_table
        .get_all_columns()
        .map(Expression::Column)
        .collect::<Vec<_>>();
    let schema = Schema::generate(
        SelectStatement::new(
            &conn,
            schema_table.root_page_id,
            columns,
            Expression::one(),
            QueryPlan::FullScan,
        ),
        schema_table,
    )
    .unwrap();
    schema.get_table(table.as_bytes()).unwrap().root_page_id
}

pub fn find_index_page_id(index: &str, filepath: &Path) -> PageId {
    let conn = Connection::open(filepath).unwrap();
    let schema_table = Schema::schema_table();
    let columns = schema_table
        .get_all_columns()
        .map(Expression::Column)
        .collect::<Vec<_>>();
    let schema = Schema::generate(
        SelectStatement::new(
            &conn,
            schema_table.root_page_id,
            columns,
            Expression::one(),
            QueryPlan::FullScan,
        ),
        schema_table,
    )
    .unwrap();
    schema.get_index(index.as_bytes()).unwrap().root_page_id
}

pub fn build_record(record: &[Option<&Value>]) -> Vec<u8> {
    let payload = RecordPayload::new(record).unwrap();
    let mut buf = vec![0; payload.size().get() as usize];
    assert_eq!(
        payload.load(0, &mut buf).unwrap(),
        payload.size().get() as usize
    );
    buf
}

pub fn build_comparators<'a>(values: &'a [Option<Value>]) -> Vec<Option<ValueCmp<'a>>> {
    values
        .iter()
        .map(|v| v.as_ref().map(|v| ValueCmp::new(v, &Collation::Binary)))
        .collect::<Vec<_>>()
}
