use super::{Error, Result};
use crate::tokens::Tokens;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::mem;
use std::ops::Range;

const COLUMN_SIZE_ID: usize = mem::size_of::<u16>();
const COLUMN_SIZE_USERNAME: usize = 32;
const COLUMN_SIZE_EMAIL: usize = 255;
pub const ROW_SIZE: usize = COLUMN_SIZE_ID + COLUMN_SIZE_USERNAME + COLUMN_SIZE_EMAIL;

const RANGE_ID: Range<usize> = 0..COLUMN_SIZE_ID;
const RANGE_USERNAME: Range<usize> = RANGE_ID.end..RANGE_ID.end + COLUMN_SIZE_USERNAME;
const RANGE_EMAIL: Range<usize> = RANGE_USERNAME.end..RANGE_USERNAME.end + COLUMN_SIZE_EMAIL;

/// Database row directly parsed from token stream where all values are stored as strings
#[derive(Debug, Eq, PartialEq)]
pub struct InputRow {
    pub id: String,
    pub username: String,
    pub email: String,
}

impl InputRow {
    pub fn parse(tokens: &mut Tokens) -> Result<Self> {
        Ok(Self {
            id: tokens
                .next()
                .ok_or(Error::ExecutionError("missing id".into()))?
                .into(),
            username: tokens
                .next()
                .ok_or(Error::ExecutionError("missing username".into()))?
                .into(),
            email: tokens
                .next()
                .ok_or(Error::ExecutionError("missing email".into()))?
                .into(),
        })
    }

    pub fn validate(&self) -> Result<ValidatedRow> {
        let mut validated = ValidatedRow {
            id: self
                .id
                .parse()
                .map_err(|e| Error::ExecutionError(format!("failed while parsing id ({})", e)))?,
            username: [0; COLUMN_SIZE_USERNAME],
            email: [0; COLUMN_SIZE_EMAIL],
        };
        if self.username.as_bytes().len() > COLUMN_SIZE_USERNAME {
            Err(Error::ExecutionError("username too long".into()))
        } else if self.email.as_bytes().len() > COLUMN_SIZE_EMAIL {
            Err(Error::ExecutionError("email too long".into()))
        } else {
            validated
                .username
                .as_mut_slice()
                .write(self.username.as_bytes())?;
            validated
                .email
                .as_mut_slice()
                .write(self.email.as_bytes())?;
            Ok(validated)
        }
    }
}

fn fixed_bytes_to_string(bytes: &[u8]) -> String {
    let last = memchr::memchr(0, bytes).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..last]).into_owned()
}

impl From<&ValidatedRow> for InputRow {
    fn from(row: &ValidatedRow) -> Self {
        Self {
            id: row.id.to_string(),
            username: fixed_bytes_to_string(&row.username),
            email: fixed_bytes_to_string(&row.email),
        }
    }
}

impl Display for InputRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{},{}", self.id, self.username, self.email)
    }
}

/// Row as stack allocated data, with values parsed and validated, read to write into table page
#[derive(Debug)]
pub struct ValidatedRow {
    id: u16,
    username: [u8; COLUMN_SIZE_USERNAME],
    email: [u8; COLUMN_SIZE_EMAIL],
}

impl ValidatedRow {
    pub fn write(&self, buffer: &mut [u8; ROW_SIZE]) -> Result<()> {
        (&mut buffer[RANGE_ID]).write(&self.id.to_be_bytes())?;
        (&mut buffer[RANGE_USERNAME]).write(&self.username)?;
        (&mut buffer[RANGE_EMAIL]).write(&self.email)?;
        Ok(())
    }

    pub fn read(buffer: &[u8; ROW_SIZE]) -> Self {
        // fine to unwrap here as the slice is guaranteed to be large enough
        Self {
            id: u16::from_be_bytes(buffer[RANGE_ID].try_into().unwrap()),
            username: buffer[RANGE_USERNAME].try_into().unwrap(),
            email: buffer[RANGE_EMAIL].try_into().unwrap(),
        }
    }
}
