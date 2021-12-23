use crate::sql::row::{self, ValidatedRow};
use crate::sql::{self, Error};
use std::iter;

pub struct Table {
    pages: Vec<Option<Page>>,
    num_rows: usize,
}

impl Table {
    pub fn new() -> Self {
        Table {
            pages: iter::repeat_with(|| None).take(MAX_PAGES).collect(),
            num_rows: 0,
        }
    }

    /// insert a row into the table
    pub fn insert(&mut self, row: &ValidatedRow) -> sql::Result<()> {
        if self.num_rows == MAX_ROWS {
            return Err(Error::TableFullError(MAX_ROWS));
        }
        let index = PageIndex::from(self.num_rows);
        if self.pages[index.page].is_none() {
            self.pages[index.page] = Some(Page::new());
        }
        row.write(&mut self.row_mut(index).unwrap())
            .map(|_| self.num_rows += 1)
    }

    fn row(&self, index: PageIndex) -> Option<&[u8; row::ROW_SIZE]> {
        let page = self.pages[index.page].as_ref()?;
        Some(page.row(index.row))
    }

    fn row_mut(&mut self, index: PageIndex) -> Option<&mut [u8; row::ROW_SIZE]> {
        let page = self.pages[index.page].as_mut()?;
        Some(page.row_mut(index.row))
    }

    /// select and return all rows from the table
    pub fn select(&mut self) -> sql::Result<Results> {
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

struct PageIndex {
    page: usize,
    row: usize,
}

impl From<usize> for PageIndex {
    fn from(row_index: usize) -> Self {
        PageIndex {
            page: row_index / ROWS_PER_PAGE,
            row: row_index % ROWS_PER_PAGE,
        }
    }
}

impl From<PageIndex> for usize {
    fn from(page_index: PageIndex) -> Self {
        page_index.page * ROWS_PER_PAGE + page_index.row
    }
}

struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    fn new() -> Self {
        let mut data: Vec<u8> = Vec::with_capacity(PAGE_SIZE);
        data.resize(PAGE_SIZE, 0);
        let data = data.into_boxed_slice();
        let pointer = Box::into_raw(data) as *mut [u8; PAGE_SIZE];
        let data = unsafe { Box::from_raw(pointer) };
        Page { data }
    }

    fn row(&self, index: usize) -> &[u8; row::ROW_SIZE] {
        let offset = row::ROW_SIZE * index;
        let buffer = &self.data[offset..offset + row::ROW_SIZE];
        buffer.try_into().unwrap()
    }

    fn row_mut(&mut self, index: usize) -> &mut [u8; row::ROW_SIZE] {
        let offset = row::ROW_SIZE * index;
        let buffer = &mut self.data[offset..offset + row::ROW_SIZE];
        buffer.try_into().unwrap()
    }
}

const PAGE_SIZE: usize = 4096;
const MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / row::ROW_SIZE;
const MAX_ROWS: usize = ROWS_PER_PAGE * MAX_PAGES;

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
        let mut table = Table::new();
        table.insert(&sample_row.validate().unwrap()).unwrap();
        let result: Vec<_> = table
            .select()
            .unwrap()
            .map(|row| InputRow::from(&row))
            .collect();
        assert_eq!(result, vec![sample_row]);
    }
}
