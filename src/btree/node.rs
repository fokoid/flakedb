// TODO: make leaf::Node private and abstract behind Node enum
// enum Node<'a> {
//     Internal(Internal),
//     Leaf(leaf::Node<'a>),
// }
//
// struct Internal;
//
pub mod common {
    pub mod header {
        use std::mem;
        use std::ops::Range;

        pub const SIZE_FLAGS: usize = mem::size_of::<u8>();
        pub const SIZE_PARENT: usize = mem::size_of::<usize>();
        pub const SIZE: usize = SIZE_FLAGS + SIZE_PARENT;

        pub const RANGE_FLAGS: Range<usize> = 0..SIZE_FLAGS;
        pub const RANGE_PARENT: Range<usize> = RANGE_FLAGS.end..RANGE_FLAGS.end + SIZE_PARENT;
    }
}

pub mod leaf {
    use crate::btree::node::leaf::body::SIZE_CELL;
    use crate::sql::pager::{self, Page, PageIndex};
    use crate::sql::row;
    use crate::sql::row::ValidatedRow;
    use crate::sql::Result;
    use std::cell::{Ref, RefMut};
    use std::io::Write;

    pub const PAGE_SPACE_FOR_CELLS: usize = pager::PAGE_SIZE - header::SIZE;
    pub const PAGE_MAX_CELLS: usize = PAGE_SPACE_FOR_CELLS / body::SIZE_CELL;

    pub mod header {
        use super::super::common;

        use std::mem;
        use std::ops::Range;

        pub const SIZE_NUM_CELLS: usize = mem::size_of::<usize>();
        pub const SIZE: usize = common::header::SIZE + SIZE_NUM_CELLS;

        pub const RANGE_NUM_CELLS: Range<usize> =
            common::header::SIZE..common::header::SIZE + SIZE_NUM_CELLS;
    }

    pub mod body {
        use super::header;
        use crate::sql::row;
        use std::mem;
        use std::ops::Range;

        pub const SIZE_KEY: usize = mem::size_of::<usize>();
        pub const SIZE_VALUE: usize = row::ROW_SIZE;
        pub const SIZE_CELL: usize = SIZE_KEY + SIZE_VALUE;

        fn cell_offset(cell_index: usize) -> usize {
            header::SIZE + cell_index * SIZE_CELL
        }

        pub fn range_key(cell_index: usize) -> Range<usize> {
            let offset = cell_offset(cell_index);
            offset..offset + SIZE_KEY
        }

        pub fn range_value(cell_index: usize) -> Range<usize> {
            let offset = cell_offset(cell_index) + SIZE_KEY;
            offset..offset + SIZE_VALUE
        }

        pub fn range_cell(cell_index: usize) -> Range<usize> {
            let offset = cell_offset(cell_index);
            offset..offset + SIZE_CELL
        }
    }

    #[derive(Copy, Clone)]
    pub struct Node<'a> {
        pager: &'a pager::Pager,
        page: pager::PageIndex,
    }

    impl<'a> Node<'a> {
        pub fn new(pager: &'a pager::Pager, page: PageIndex) -> Result<Self> {
            Ok(Self { pager, page })
        }

        fn borrow_page(&self) -> Result<Ref<'a, Page>> {
            self.pager.borrow_page(self.page)
        }

        fn borrow_page_mut(&self) -> Result<RefMut<'a, Page>> {
            self.pager.borrow_page_mut(self.page)
        }

        pub fn num_cells(&self) -> Result<usize> {
            let page = self.borrow_page()?;
            let slice = &page.as_slice()[header::RANGE_NUM_CELLS];
            let num_cells = usize::from_be_bytes(slice.try_into().unwrap());
            Ok(num_cells)
        }

        fn set_num_cells(&mut self, num_cells: usize) -> Result<()> {
            let mut page = self.borrow_page_mut()?;
            let mut slice = &mut page.as_mut_slice()[header::RANGE_NUM_CELLS];
            slice.write(&num_cells.to_be_bytes())?;
            Ok(())
        }

        pub fn entry(&self, index: usize) -> Result<Entry<'a>> {
            if index >= self.num_cells()? {
                panic!(
                    "attempted to access out of bounds cell {} ({} cells in page)",
                    index,
                    self.num_cells()?
                );
            }
            Ok(Entry { node: *self, index })
        }

        pub fn insert(&mut self, index: usize, key: usize, row: &ValidatedRow) -> Result<()> {
            let num_cells = self.num_cells()?;
            if num_cells >= PAGE_MAX_CELLS {
                unimplemented!("cell is full, splitting not yet implemented");
            }
            if index < num_cells {
                // move cells to make space
                let mut page = self.borrow_page_mut()?;
                for cell_num in num_cells..index {
                    let (source, mut target) =
                        page.as_mut_slice().split_at_mut(cell_num * SIZE_CELL);
                    target.write(&source[source.len() - SIZE_CELL..])?;
                }
            }
            let mut entry = Entry { node: *self, index };
            entry.set_key(key)?;
            row.write(&mut *entry.value_mut()?)?;
            self.set_num_cells(num_cells + 1)?;
            Ok(())
        }
    }

    pub struct Entry<'a> {
        // wanted to use a reference here, but that ties the lifetime of the Entry to the lifetime
        // of a specific node, rather than simply the lifetime of the pager
        node: Node<'a>,
        index: usize,
    }

    impl<'a> Entry<'a> {
        pub fn key(&self) -> Result<usize> {
            let page = self.node.borrow_page()?;
            let slice = &page.as_slice()[body::range_key(self.index)];
            Ok(usize::from_be_bytes(slice.try_into().unwrap()))
        }

        pub fn set_key(&mut self, key: usize) -> Result<()> {
            let mut page = self.node.borrow_page_mut()?;
            let mut slice = &mut page.as_mut_slice()[body::range_key(self.index)];
            slice.write(&key.to_be_bytes())?;
            Ok(())
        }

        pub fn value(&self) -> Result<Ref<'a, [u8; row::ROW_SIZE]>> {
            let page: Ref<'a, Page> = self.node.borrow_page()?;
            let value: Ref<'a, [u8; row::ROW_SIZE]> = Ref::map(page, |page| {
                let slice = &page.as_slice()[body::range_value(self.index)];
                slice.try_into().unwrap()
            });
            Ok(value)
        }

        pub fn value_mut(&mut self) -> Result<RefMut<'a, [u8; row::ROW_SIZE]>> {
            let page = self.node.borrow_page_mut()?;
            let value = RefMut::map(page, |page| {
                let slice = &mut page.as_mut_slice()[body::range_value(self.index)];
                slice.try_into().unwrap()
            });
            Ok(value)
        }
    }
}
