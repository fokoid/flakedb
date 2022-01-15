use crate::btree::Node;
use super::pager::Pager;
use super::row::{InputRow, ValidatedRow};
use super::table::{Results, Table};
use super::statement::Statement;
use super::Result;
use std::path::PathBuf;

pub struct Database {
    pager: Pager,
    tables: Vec<Table>,
}

// TODO: cleanup messy API
impl Database {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        let pager = Pager::open(path)?;
        let mut db = Self { pager, tables: Vec::new() };
        db.create_table()?;
        Ok(db)
    }

    pub fn execute(&mut self, statement: &Statement) -> Result<()> {
        eprintln!("Executing: {:?}", statement);
        match statement {
            Statement::Insert(row) => {
                self.insert(&row.validate()?)?;
                Ok(())
            }
            Statement::Select => {
                // TODO: move output handling out of Database
                for row in self.select()? {
                    let row = InputRow::from(&row);
                    println!("{}", row);
                }
                Ok(())
            }
            Statement::None => Ok(()),
        }
    }

    pub fn insert(&self, row: &ValidatedRow) -> Result<()> {
        self.get_table().insert(&self.pager, row)
    }

    pub fn select(&self) -> Result<Results> {
        self.get_table().select(&self.pager)
    }

    fn create_table(&mut self) -> Result<()> {
        // single fixed schema table for now
        if self.tables.is_empty() {
            self.tables.push(Table{ root: 0 });
        }
        Ok(())
    }

    pub fn get_table(&self) -> &Table {
        &self.tables[0]
    }

    pub fn get_table_root(&self) -> Result<Node> {
        self.get_table().root(&self.pager)
    }
}
