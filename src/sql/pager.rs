use crate::sql::Result;
use std::cell::{Ref, RefCell, RefMut};
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
    pages: Vec<RefCell<Option<Page>>>,
    file: Option<RefCell<PageFile>>,
}

impl Pager {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        let file = if let Some(path) = path {
            Some(RefCell::new(PageFile::open(path)?))
        } else {
            None
        };
        Ok(Self {
            pages: iter::repeat_with(|| RefCell::new(None)).take(MAX_PAGES).collect(),
            file,
        })
    }

    pub fn len(&self) -> usize {
        self.file.as_ref().map_or(0, |file| file.borrow().len)
    }

    fn load_page_if_missing(&self, index: usize) -> Result<()> {
        if index > MAX_PAGES {
            panic!("page {} out of bounds (max {})", index, MAX_PAGES);
        }
        let mut page = self.pages[index].borrow_mut();
        if page.is_none() {
            *page = Some(if let Some(file) = &self.file {
                let mut file = file.borrow_mut();
                let offset = index * PAGE_SIZE;
                if offset + PAGE_SIZE <= file.len {
                    Page::from_file(&mut file, offset)?
                } else {
                    Page::new()
                }
            } else {
                Page::new()
            });
        };
        Ok(())
    }

    pub fn borrow_page(&self, index: usize) -> Result<Ref<Page>> {
        self.load_page_if_missing(index)?;
        Ok(Ref::map(self.pages[index].borrow(), |page| page.as_ref().unwrap()))
    }

    pub fn borrow_page_mut(&self, index: usize) -> Result<RefMut<Page>> {
        self.load_page_if_missing(index)?;
        Ok(RefMut::map(self.pages[index].borrow_mut(), |page| page.as_mut().unwrap()))
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        if let Some(file) = &self.file {
            let mut file = file.borrow_mut();
            for (i, page) in self.pages.iter().enumerate() {
                let page = page.borrow();
                if let Some(page) = page.as_ref() {
                    if let Err(error) = page.to_file(&mut file, PAGE_SIZE * i) {
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

#[derive(Debug)]
pub struct Page {
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

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut[u8] {
        self.data.as_mut_slice()
    }
}

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 100;
