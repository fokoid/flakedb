use crate::sql::row;
use crate::sql::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter;
use std::path::PathBuf;
struct PageFile {
    file: File,
    len: usize,
}

impl PageFile {
    fn open(path: &PathBuf) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let len = file.seek(SeekFrom::End(0))? as usize;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file, len })
    }

    fn grow(&mut self, new_len: usize) -> Result<()> {
        if new_len > self.len {
            self.file.set_len(new_len as u64)?;
            self.len = new_len;
        }
        Ok(())
    }
}

pub struct Pager {
    pages: Vec<Option<Page>>,
    file: Option<PageFile>,
}

impl Pager {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        let file = if let Some(path) = path {
            Some(PageFile::open(path)?)
        } else {
            None
        };
        Ok(Self {
            pages: iter::repeat_with(|| None).take(MAX_PAGES).collect(),
            file,
        })
    }

    pub fn len(&self) -> usize {
        self.file.as_ref().map_or(0, |file| file.len)
    }

    fn page(&mut self, index: usize) -> Result<&mut Page> {
        if index > MAX_PAGES {
            panic!("page {} out of bounds (max {})", index, MAX_PAGES);
        }
        if self.pages[index].is_none() {
            self.pages[index] = Some(if let Some(file) = &mut self.file {
                let offset = index * PAGE_SIZE;
                if offset + PAGE_SIZE <= file.len {
                    Page::from_file(file, offset)?
                } else {
                    Page::new()
                }
            } else {
                Page::new()
            })
        };
        Ok(self.pages[index].as_mut().unwrap())
    }

    pub fn row(&mut self, index: usize) -> Result<&[u8; row::ROW_SIZE]> {
        let index = PageIndex::from(index);
        self.page(index.page).map(|page| page.row(index.row))
    }

    pub fn row_mut(&mut self, index: usize) -> Result<&mut [u8; row::ROW_SIZE]> {
        let index = PageIndex::from(index);
        match self.page(index.page) {
            Ok(page) => Ok(page.row_mut(index.row)),
            Err(err) => Err(err),
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        if let Some(file) = &mut self.file {
            for (i, page) in self.pages.iter_mut().enumerate() {
                if let Some(page) = page {
                    if let Err(error) = page.to_file(file, PAGE_SIZE * i) {
                        eprintln!(
                            "WARN: possible data loss. Error flushing page {} to disk ({}).",
                            i, error
                        );
                    }
                }
            }
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

#[derive(Debug)]
struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Create new page by taking ownership of vector
    fn from_vec(data: Vec<u8>) -> Self {
        if data.len() != PAGE_SIZE {
            panic!(
                "Attempted to build page from {} bytes (page size is {}).",
                data.len(),
                PAGE_SIZE
            );
        }
        let data = data.into_boxed_slice();
        let pointer = Box::into_raw(data) as *mut [u8; PAGE_SIZE];
        let data = unsafe { Box::from_raw(pointer) };
        Page { data }
    }

    /// Create new page by zeroing memory
    fn new() -> Self {
        let mut data: Vec<u8> = Vec::with_capacity(PAGE_SIZE);
        data.resize(PAGE_SIZE, 0);
        Self::from_vec(data)
    }

    /// Create new page by copying byte array
    fn from_file(file: &mut PageFile, offset: usize) -> Result<Self> {
        let mut data = Vec::with_capacity(PAGE_SIZE);
        data.resize(PAGE_SIZE, 0);
        eprintln!(
            "Reading {} bytes at offset {} (total {}).",
            data.len(),
            offset,
            file.len
        );
        file.file.seek(SeekFrom::Start(offset as u64))?;
        file.file.read_exact(data.as_mut_slice())?;
        Ok(Self::from_vec(data))
    }

    fn to_file(&self, file: &mut PageFile, offset: usize) -> Result<()> {
        file.grow(offset + PAGE_SIZE)?;
        file.file.seek(SeekFrom::Start(offset as u64))?;
        let bytes_written = file.file.write(self.data.as_slice())?;
        if bytes_written != PAGE_SIZE {
            panic!(
                "Wrote {} bytes but PAGE_SIZE is {}.",
                bytes_written, PAGE_SIZE
            );
        }
        Ok(())
    }

    fn row(&self, index: usize) -> &[u8; row::ROW_SIZE] {
        if index > ROWS_PER_PAGE {
            panic!(
                "row {} out of bounds (max per page {})",
                index, ROWS_PER_PAGE
            );
        }
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

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / row::ROW_SIZE;
