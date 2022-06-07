use crate::sql::pager::{Pager, PageIndex};
use crate::sql::{Error, Result};
use common::header::NodeType;
use std::fmt::{Display, Formatter};

pub enum Node<'a> {
    Internal(internal::Node),
    Leaf(leaf::Node<'a>),
}

impl<'a> Node<'a> {
    pub fn new(pager: &'a Pager, page_index: PageIndex) -> Result<Self> {
        let flags = {
            let page = pager.borrow_page(page_index)?;
            common::header::flags(page)
        };
        match flags.node_type {
            NodeType::Internal => Ok(Self::Internal(internal::Node)),
            NodeType::Leaf => Ok(Self::Leaf(leaf::Node::new(pager, page_index))),
            NodeType::Unknown(u) => Err(Error::PageCorrupt(
                format!("unknown node format {}", u)
            )),
        }
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(match self {
            Node::Leaf(leaf) => leaf.num_cells()? == 0,
            _ => unimplemented!()
        })
    }
}

impl<'a> Display for Node<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(_) => write!(f, "Internal node"),
            Self::Leaf(leaf) => write!(f, "Leaf Node: {}", leaf),
        }
    }
}
pub mod internal {
    pub struct Node;
}

pub mod common {
    pub mod header {
        use std::cell::Ref;
        use std::mem;
        use std::ops::Range;
        use crate::sql::pager::Page;

        pub enum NodeType {
            Root,
            Leaf,
            Internal,
            Unknown(u8),
        }

        pub struct Flags {
            pub node_type: NodeType,
        }

        const NODE_TYPE_ROOT: u8 = 1;
        const NODE_TYPE_INTERNAL: u8 = 2;
        const NODE_TYPE_LEAF: u8 = 0;

        impl From<u8> for Flags {
            fn from(flags: u8) -> Self {
                Self {
                    node_type: match flags & FLAG_MASK_NODE_TYPE {
                        NODE_TYPE_ROOT => NodeType::Root,
                        NODE_TYPE_INTERNAL=> NodeType::Internal,
                        NODE_TYPE_LEAF => NodeType::Leaf,
                        x => NodeType::Unknown(x),
                    }
                }
            }
        }

        impl From<Flags> for u8 {
            fn from(flags: Flags) -> Self {
                let result = match flags.node_type {
                    NodeType::Root => NODE_TYPE_ROOT,
                    NodeType::Internal => NODE_TYPE_INTERNAL,
                    NodeType::Leaf => NODE_TYPE_LEAF,
                    NodeType::Unknown(_) => panic!("unknown node type"),
                };
                result
            }
        }

        pub const FLAG_MASK_NODE_TYPE: u8 = 3;
        pub const SIZE_FLAGS: usize = mem::size_of::<u8>();
        pub const SIZE_PARENT: usize = mem::size_of::<usize>();
        pub const SIZE: usize = SIZE_FLAGS + SIZE_PARENT;

        pub const RANGE_FLAGS: Range<usize> = 0..SIZE_FLAGS;
        pub const RANGE_PARENT: Range<usize> = RANGE_FLAGS.end..RANGE_FLAGS.end + SIZE_PARENT;

        pub fn flags(page: Ref<Page>) -> Flags {
            let slice = &page.as_slice()[RANGE_FLAGS];
            u8::from_be_bytes(slice.try_into().unwrap()).into()
        }
    }
}

pub mod leaf {
    use crate::btree::node::leaf::body::SIZE_CELL;
    use crate::sql::pager::{self, Page, PageIndex};
    use crate::sql::row;
    use crate::sql::row::ValidatedRow;
    use crate::sql::Result;
    use std::cell::{Ref, RefMut};
    use std::fmt::{Display, Formatter};
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
        pub fn new(pager: &'a pager::Pager, page: PageIndex) -> Self {
            Self { pager, page }
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

    impl<'a> Display for Node<'a> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            if let Ok(num_cells) = self.num_cells() {
                let keys = (0..num_cells).map(|index| {
                    let entry = self.entry(index).unwrap();
                    entry.key().map_or(String::from("?"), |key| format!("{}", key))
                }).collect::<Vec<_>>().join(", ");
                write!(f, "Leaf Node ({} cells, keys: [{}])", num_cells, keys)?;
            } else {
                write!(f, "Leaf Node (Invalid/Unknown)")?;
            }
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
