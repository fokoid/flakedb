use crate::sql::{pager, row};
use crate::btree::node::leaf;
use const_format::formatcp;

pub use pager::{PAGE_SIZE, MAX_PAGES};
pub use row::ROW_SIZE;
pub use leaf::{PAGE_MAX_CELLS, PAGE_SPACE_FOR_CELLS};
pub use leaf::header::SIZE as LEAF_HEADER_SIZE;
pub use leaf::body::SIZE_CELL;

pub const CONSTANTS: [(&str, &str, &str); 7] = [
    ("pager", "PAGE_SIZE", formatcp!("{}", PAGE_SIZE)),
    ("pager", "MAX_PAGES", formatcp!("{}", MAX_PAGES)),
    ("row", "ROW_SIZE", formatcp!("{}", ROW_SIZE)),
    ("leaf", "HEADER_SIZE", formatcp!("{}", LEAF_HEADER_SIZE)),
    ("leaf", "PAGE_SPACE_FOR_CELLS", formatcp!("{}", PAGE_SPACE_FOR_CELLS)),
    ("leaf", "CELL_SIZE", formatcp!("{}", SIZE_CELL)),
    ("leaf", "MAX_CELLS_PER_PAGE", formatcp!("{}", PAGE_MAX_CELLS)),
];
