use super::row::ValidatedRow;
use super::table::{Results, Table};
use super::Result;
use std::path::PathBuf;

pub struct Database {
    // single fixed schema table for now
    table: Table,
}

impl Database {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        Ok(Self {
            table: Table::open(path)?,
        })
    }

    pub fn insert(&mut self, row: &ValidatedRow) -> Result<()> {
        self.table.insert(row)
    }

    pub fn select(&mut self) -> Result<Results> {
        self.table.select()
    }

    pub fn get_table(&self) -> &Table {
        &self.table
    }
}
