use std::cell::Ref;
use crate::sql::row::{self, ValidatedRow};
use crate::sql::Result;
use crate::sql::pager::Pager;
use crate::sql::db::Table;
use crate::btree::Node;

pub struct Cursor<'a> {
    node: Node<'a>,
    cell_index: usize,
    at_end: bool,
}

impl<'a> Cursor<'a> {
    pub fn start(table: &Table, pager: &'a Pager) -> Result<Self> {
        let node = Node::new(pager, table.root)?;
        let at_end = node.is_empty()?;
        Ok(Self { node, cell_index: 0, at_end })
    }

    pub fn at(table: &Table, pager: &'a Pager, key: usize) -> Result<Self> {
        let root = Node::new(pager, table.root)?;
        let cursor = match root {
            Node::Leaf(node) => {
                // binary search for largest index such that key at index <= search key
                let num_cells = node.num_cells()?;
                let mut search_range = 0..num_cells;
                while search_range.len() != 0 {
                    let index = (search_range.start + search_range.end) / 2;
                    let key_at_index = node.entry(index)?.key()?;
                    search_range = if key_at_index < key {
                        index..search_range.end
                    } else if key_at_index > key {
                        search_range.start..index
                    } else {
                        index..index
                    };
                };
                let cell_index = search_range.start;
                Self { node: root, cell_index, at_end: cell_index == num_cells, }
            },
            Node::Internal(node) => {
                unimplemented!("cursor at non leaf node")
            }
        };
        Ok(cursor)
    }

    fn row(&self) -> Result<Ref<'a, [u8; row::ROW_SIZE]>> {
        if let Node::Leaf(node) = &self.node {
            node.entry(self.cell_index)?.value()
        } else {
            unimplemented!("cursor at non leaf node")
        }
    }

    pub fn insert(&mut self, key: usize, row: &ValidatedRow) -> Result<()> {
        match &mut self.node {
            Node::Leaf(node) => {
                node.insert(self.cell_index, key, row)?;
            },
            Node::Internal(node) => {
                unimplemented!("cursor at non leaf node")
            }
        };
        Ok(())
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Result<Ref<'a, [u8; row::ROW_SIZE]>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.at_end {
            let row = Some(self.row());
            self.cell_index += 1;
            if let Node::Leaf(leaf) = &self.node{
                match leaf.num_cells() {
                    Ok(num_cells) => {
                        if self.cell_index == num_cells {
                            self.at_end = true;
                        }
                        row
                    }
                    Err(err) => Some(Err(err)),
                }
            } else {
                unimplemented!("cursor at non leaf node")
            }
        } else {
            None
        }
    }
}
