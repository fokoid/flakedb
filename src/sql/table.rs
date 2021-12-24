use crate::sql::pager::{self, Pager};
use crate::sql::row::{self, ValidatedRow};
use crate::sql::{Error, Result};
use std::path::PathBuf;

pub struct Table {
    pager: Pager,
    num_rows: usize,
}

impl Table {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        let pager = Pager::open(path)?;
        let num_rows = pager.len() / row::ROW_SIZE;
        Ok(Table { pager, num_rows })
    }

    /// insert a row into the table
    pub fn insert(&mut self, row: &ValidatedRow) -> Result<()> {
        if self.num_rows == MAX_ROWS {
            return Err(Error::TableFullError(MAX_ROWS));
        }
        row.write(self.row_mut(self.num_rows)?)
            .map(|_| self.num_rows += 1)
    }

    fn row(&mut self, index: usize) -> Result<&[u8; row::ROW_SIZE]> {
        self.pager.row(index)
    }

    fn row_mut(&mut self, index: usize) -> Result<&mut [u8; row::ROW_SIZE]> {
        self.pager.row_mut(index)
    }

    /// select and return all rows from the table
    pub fn select(&mut self) -> Result<Results> {
        Ok(Results::new(self))
    }
}

pub struct Results<'a> {
    next_row: usize,
    table: &'a mut Table,
}

impl<'a> Results<'a> {
    fn new(table: &'a mut Table) -> Self {
        Self { next_row: 0, table }
    }
}

impl<'a> Iterator for Results<'a> {
    type Item = ValidatedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row < self.table.num_rows {
            let buffer = self.table.row(self.next_row.into()).unwrap();
            self.next_row += 1;
            Some(ValidatedRow::read(buffer))
        } else {
            None
        }
    }
}

const MAX_ROWS: usize = pager::ROWS_PER_PAGE * pager::MAX_PAGES;

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
        let mut table = Table::open(None).unwrap();
        table.insert(&sample_row.validate().unwrap()).unwrap();
        let result: Vec<_> = table
            .select()
            .unwrap()
            .map(|row| InputRow::from(&row))
            .collect();
        assert_eq!(result, vec![sample_row]);
    }
}
