use crate::btree::Node;
use crate::sql::pager::{PageIndex, Pager};
use crate::sql::row::{self, ValidatedRow};
use crate::sql::Result;
use std::cell::Ref;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

pub struct Table {
    pager: Pager,
    root: PageIndex,
}

impl Table {
    pub fn open(path: Option<&PathBuf>) -> Result<Self> {
        let pager = Pager::open(path)?;
        Ok(Table { pager, root: 0 })
    }

    /// insert a row into the table
    pub fn insert(&mut self, row: &ValidatedRow) -> Result<()> {
        let mut cursor = Cursor::end(&self)?;
        cursor.insert(row.key(), row)?;
        Ok(())
    }

    /// select and return all rows from the table
    pub fn select(&self) -> Result<Results> {
        let cursor = Cursor::start(&self)?;
        Ok(Results::new(cursor))
    }

    fn root(&self) -> Result<Node> {
        Node::new(&self.pager, self.root)
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.root() {
            Ok(node) => write!(f, "Root: {}", &node),
            Err(err) => write!(f, "{:?}", err),
        }
    }
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

struct Cursor<'a> {
    node: Node<'a>,
    cell_index: usize,
    at_end: bool,
}

impl<'a> Cursor<'a> {
    pub fn start(table: &'a Table) -> Result<Self> {
        let node = Node::new(&table.pager, table.root)?;
        let at_end = node.is_empty()?;
        Ok(Self { node, cell_index: 0, at_end, })
    }

    pub fn end(table: &'a Table) -> Result<Self> {
        let node = Node::new(&table.pager, table.root)?;
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

    fn insert(&mut self, key: usize, row: &ValidatedRow) -> Result<()> {
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
