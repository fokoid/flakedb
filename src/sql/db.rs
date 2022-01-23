use crate::btree::Node;
use super::pager::{Pager, PageIndex};
use super::row::{InputRow, ValidatedRow};
use super::statement::Statement;
use crate::sql::Result;
use crate::sql::cursor::Cursor;
use std::path::PathBuf;

pub struct Table {
    pub root: PageIndex,
}

pub struct Database {
    pager: Pager,
    tables: Vec<Table>,
}

pub struct Results<'a> {
    cursor: Cursor<'a>,
}

impl<'a> Results<'a> {
    fn new(cursor: Cursor<'a>) -> Self {
        Self { cursor }
    }
}

impl<'a> Iterator for Results<'a> {
    type Item = ValidatedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(buffer) = self.cursor.next() {
            let buffer = *buffer.unwrap();
            Some(ValidatedRow::read(&buffer))
        } else {
            None
        }
    }
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

    /// insert a row into the table
    pub fn insert(&self, row: &ValidatedRow) -> Result<()> {
        let mut cursor = Cursor::end(self.get_table(), &self.pager)?;
        cursor.insert(row.key(), row)?;
        Ok(())
    }

    /// select and return all rows from the table
    pub fn select(&self) -> Result<Results> {
        let cursor = Cursor::start(self.get_table(), &self.pager)?;
        Ok(Results::new(cursor))
    }

    fn create_table(&mut self) -> Result<()> {
        // single fixed schema table for now
        if self.tables.is_empty() {
            self.tables.push(Table{ root: 0 });
        }
        Ok(())
    }

    fn get_table(&self) -> &Table {
        &self.tables[0]
    }

    pub fn tree_as_string(&self) -> Result<String> {
        let root_node = Node::new(&self.pager, self.get_table().root)?;
        Ok(format!("Root: {}", root_node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::InputRow;

    #[test]
    fn insert_and_select() {
        let sample_row = InputRow {
            id: "1".into(),
            username: "karl".into(),
            email: "karl.havok@hotmail.com".into(),
        };
        let mut db = Database::open(None).unwrap();
        db.insert(&sample_row.validate().unwrap()).unwrap();
        let result: Vec<_> = db
            .select()
            .unwrap()
            .map(|row| InputRow::from(&row))
            .collect();
        assert_eq!(result, vec![sample_row]);
    }
}