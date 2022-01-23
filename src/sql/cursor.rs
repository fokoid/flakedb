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
        Ok(Self { node, cell_index: 0, at_end, })
    }

    pub fn end(table: &Table, pager: &'a Pager) -> Result<Self> {
        let node = Node::new(pager, table.root)?;
        if let Node::Leaf(leaf) = node {
            Ok(Self {
                node,
                cell_index: leaf.num_cells()?,
                at_end: true,
            })
        } else {
            unimplemented!("cursor at non leaf node")
        }
    }

    fn row(&self) -> Result<Ref<'a, [u8; row::ROW_SIZE]>> {
        if let Node::Leaf(node) = &self.node {
            node.entry(self.cell_index)?.value()
        } else {
            unimplemented!("cursor at non leaf node")
        }
    }

    pub fn insert(&mut self, key: usize, row: &ValidatedRow) -> Result<()> {
        if let Node::Leaf(leaf) = &mut self.node {
            leaf.insert(leaf.num_cells()?, key, row)?;
        } else {
            unimplemented!("cursor at non leaf node")
        }
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
