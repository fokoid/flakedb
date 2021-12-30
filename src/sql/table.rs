use crate::sql::pager::{self, Pager};
use crate::sql::row::{self, ValidatedRow};
use crate::sql::{Error, Result};
use std::cell::{Ref, RefMut};
use std::ops::Range;
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

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// insert a row into the table
    pub fn insert(&mut self, row: &ValidatedRow) -> Result<()> {
        if self.num_rows == MAX_ROWS {
            return Err(Error::TableFullError(MAX_ROWS));
        }
        let result = {
            let mut cursor = Cursor::end(&self);
            let mut slice = &mut *cursor.row_mut()?;
            let result = row.write(&mut slice);
            result
        };
        // only increment row counter if insert succeeded
        let result = result.map(|_| self.num_rows += 1);
        result
    }

    /// select and return all rows from the table
    pub fn select(&self) -> Result<Results> {
        Ok(Results::new(Cursor::start(&self)))
    }
}

pub struct Results<'a> {
    cursor: Cursor<'a>
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

struct Cursor<'a> {
    table: &'a Table,
    row_num: usize,
}

impl<'a> Cursor<'a> {
    pub fn start(table: &'a Table) -> Self {
        Self { table, row_num: 0 }
    }

    pub fn end(table: &'a Table) -> Self {
        Self { table, row_num: table.num_rows() }
    }

    pub fn at_end(&self) -> bool {
        self.row_num == self.table.num_rows()
    }

    fn row(&self) -> Result<Ref<'a, [u8; row::ROW_SIZE]>> {
        let index = PageIndex::from(self.row_num);
        let page = self.table.pager.borrow_page(index.page)?;
        let row = Ref::map(page, |page| {
            let slice = &page.as_slice()[index.range()];
            let buffer: &[u8; row::ROW_SIZE] = slice.try_into().unwrap();
            buffer
        });
        Ok(row)
    }

    fn row_mut(&mut self) -> Result<RefMut<'a, [u8; row::ROW_SIZE]>> {
        let index = PageIndex::from(self.row_num);
        let page = self.table.pager.borrow_page_mut(index.page)?;
        let row = RefMut::map(page, |page| {
            let slice = &mut page.as_mut_slice()[index.range()];
            let buffer: &mut [u8; row::ROW_SIZE] = slice.try_into().unwrap();
            buffer
        });
        Ok(row)
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Result<Ref<'a, [u8; row::ROW_SIZE]>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.at_end() {
            let row = Some(self.row());
            self.row_num += 1;
            row
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct PageIndex {
    page: usize,
    offset: usize,
}

impl PageIndex {
    fn range(&self) -> Range<usize> {
        let end = self.offset + row::ROW_SIZE;
        self.offset..end
    }
}

impl From<usize> for PageIndex {
    fn from(row_index: usize) -> Self {
        PageIndex {
            page: row_index / ROWS_PER_PAGE,
            offset: row::ROW_SIZE * (row_index % ROWS_PER_PAGE),
        }
    }
}

impl From<PageIndex> for usize {
    fn from(page_index: PageIndex) -> Self {
        page_index.page * ROWS_PER_PAGE + (page_index.offset / row::ROW_SIZE)
    }
}

const ROWS_PER_PAGE: usize = pager::PAGE_SIZE / row::ROW_SIZE;
const MAX_ROWS: usize = ROWS_PER_PAGE * pager::MAX_PAGES;

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
