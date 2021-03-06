use std::result;
use std::fmt;
use std::io::{Write, Read, Cursor};
use std::iter::{FromIterator, Extend};
use std::cmp::Ordering;
use std::ops::RangeFull;

use indexmap::IndexMap;
use chrono::{DateTime, Utc};
use byteorder::WriteBytesExt;

use crate::value::{Value, Array};
use crate::encode::{encode_document, encode_bson, write_i32, EncodeResult};
use crate::decode::{decode_document, DecodeResult};
use crate::spec::BinarySubtype;
use crate::object_id::ObjectId;

pub use indexmap::map::{IntoIter, Iter, IterMut, Entry, Keys, Values, ValuesMut, Drain};

#[derive(PartialEq, Debug)]
pub enum Error {
    NotPresent,
    UnexpectedType,
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Clone, PartialEq, Eq, Default)]
pub struct Document {
    inner: IndexMap<String, Value>
}

impl Document {
    pub fn new() -> Document {
        Document {
            inner: IndexMap::new()
        }
    }

    pub fn with_capacity(n: usize) -> Document {
        Document {
            inner: IndexMap::with_capacity(n)
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.inner.get(key)
    }

    pub fn get_full(&self, key: &str) -> Option<(usize, &String, &Value)> {
        self.inner.get_full(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.inner.get_mut(key)
    }

    pub fn get_mut_full(&mut self, key: &str) -> Option<(usize, &String, &mut Value)> {
        self.inner.get_full_mut(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn entry(&mut self, key: String) -> Entry<String, Value> {
        self.inner.entry(key)
    }

    pub fn insert_value(&mut self, key: String, value: Value) -> Option<Value> {
        self.inner.insert(key, value)
    }

    pub fn insert_value_full(&mut self, key: String, value: Value) -> (usize, Option<Value>) {
        self.inner.insert_full(key, value)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.insert_value(key.into(), value.into())
    }

    pub fn insert_full(&mut self, key: impl Into<String>, value: impl Into<Value>) -> (usize, Option<Value>) {
        self.insert_value_full(key.into(), value.into())
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.inner.remove(key)
    }

    pub fn swap_remove(&mut self, key: &str) -> Option<Value> {
        self.inner.swap_remove(key)
    }

    pub fn swap_remove_full(&mut self, key: &str) -> Option<(usize, String, Value)> {
        self.inner.swap_remove_full(key)
    }

    pub fn pop(&mut self) -> Option<(String, Value)> {
        self.inner.pop()
    }

    pub fn retain<F>(&mut self, keep: F)
        where F: FnMut(&String, &mut Value) -> bool
    {
        self.inner.retain(keep)
    }

    pub fn sort_keys(&mut self) {
        self.inner.sort_keys()
    }

    pub fn sort_by<F>(&mut self, compare: F)
        where F: FnMut(&String, &Value, &String, &Value) -> Ordering
    {
        self.inner.sort_by(compare)
    }

    pub fn sorted_by<F>(self, compare: F) -> IntoIter<String, Value>
        where F: FnMut(&String, &Value, &String, &Value) -> Ordering
    {
        self.inner.sorted_by(compare)
    }

    pub fn drain(&mut self, range: RangeFull) -> Drain<String, Value> {
        self.inner.drain(range)
    }

    pub fn iter(&self) -> Iter<'_, String, Value> {
        self.into_iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, String, Value> {
        self.into_iter()
    }

    pub fn keys(&self) -> Keys<String, Value> {
        self.inner.keys()
    }

    pub fn value(&self) -> Values<String, Value> {
        self.inner.values()
    }

    pub fn value_mut(&mut self) -> ValuesMut<String, Value> {
        self.inner.values_mut()
    }

    pub fn get_f64(&self, key: &str) -> Result<f64> {
        match self.get(key) {
            Some(&Value::Double(v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_i32(&self, key: &str) -> Result<i32> {
        match self.get(key) {
            Some(&Value::Int32(v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_i64(&self, key: &str) -> Result<i64> {
        match self.get(key) {
            Some(&Value::Int64(v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_str(&self, key: &str) -> Result<&str> {
        match self.get(key) {
            Some(&Value::String(ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_array(&self, key: &str) -> Result<&Array> {
        match self.get(key) {
            Some(&Value::Array(ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_document(&self, key: &str) -> Result<&Document> {
        match self.get(key) {
            Some(&Value::Document(ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_bool(&self, key: &str) -> Result<bool> {
        match self.get(key) {
            Some(&Value::Boolean(v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn is_null(&self, key: &str) -> bool {
        self.get(key) == Some(&Value::Null)
    }

    pub fn get_binary(&self, key: &str) -> Result<&Vec<u8>> {
        match self.get(key) {
            Some(&Value::Binary(BinarySubtype::Generic, ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_object_id(&self, key: &str) -> Result<&ObjectId> {
        match self.get(key) {
            Some(&Value::ObjectId(ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_time_stamp(&self, key: &str) -> Result<u64> {
        match self.get(key) {
            Some(&Value::TimeStamp(v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn get_utc_datetime(&self, key: &str) -> Result<&DateTime<Utc>> {
        match self.get(key) {
            Some(&Value::UTCDatetime(ref v)) => Ok(v),
            Some(_) => Err(Error::UnexpectedType),
            None => Err(Error::NotPresent),
        }
    }

    pub fn encode(&self, writer: &mut impl Write) -> EncodeResult<()> {
        encode_document(writer, self)
    }

    pub fn decode(reader: &mut impl Read) -> DecodeResult<Document> {
        decode_document(reader)
    }

    pub fn to_vec(&self) -> EncodeResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(64);
        write_i32(&mut buf, 0)?;

        for (key, val) in self {
            encode_bson(&mut buf, key.as_ref(), val)?;
        }

        buf.write_u8(0)?;

        let len_bytes = (buf.len() as i32).to_le_bytes();

        buf[..4].clone_from_slice(&len_bytes);

        Ok(buf)
    }

    pub fn from_slice(slice: &[u8]) -> DecodeResult<Document> {
        let mut reader = Cursor::new(slice);
        decode_document(&mut reader)
    }

    pub fn extend(&mut self, iter: impl Into<Document>) {
        self.inner.extend(iter.into());
    }

    pub fn get_index(&self, index: usize) -> Option<(&String, &Value)> {
        self.inner.get_index(index)
    }

    pub fn get_index_mut(&mut self, index: usize) -> Option<(&mut String, &mut Value)> {
        self.inner.get_index_mut(index)
    }

    pub fn swap_remove_index(&mut self, index: usize) -> Option<(String, Value)> {
        self.inner.swap_remove_index(index)
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Document({:?})", self.inner)
    }
}

impl fmt::Display for Document {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{{")?;

        let mut first = true;
        for (k, v) in self.iter() {
            if first {
                first = false;
                write!(fmt, " ")?;
            } else {
                write!(fmt, ", ")?;
            }

            write!(fmt, "{}: {}", k, v)?;
        }

        write!(fmt, "{}}}", if !first { " " } else { "" })?;

        Ok(())
    }
}

impl IntoIterator for Document {
    type Item = (String, Value);
    type IntoIter = IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a Document {
    type Item = (&'a String, &'a Value);
    type IntoIter = Iter<'a, String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a> IntoIterator for &'a mut Document {
    type Item = (&'a String, &'a mut Value);
    type IntoIter = IterMut<'a, String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl FromIterator<(String, Value)> for Document {
    fn from_iter<I: IntoIterator<Item=(String, Value)>>(iter: I) -> Self {
        let mut document = Document::with_capacity(8);

        for (k, v) in iter {
            document.insert(k, v);
        }

        document
    }
}

impl From<IndexMap<String, Value>> for Document {
    fn from(map: IndexMap<String, Value>) -> Document {
        Document { inner: map }
    }
}

#[cfg(test)]
mod test {
    use crate::Document;
    use crate::doc;

    #[test]
    fn to_vec() {
        let document = doc!{"aa": "bb"};

        let vec = document.to_vec().unwrap();

        let document2 = Document::from_slice(&vec).unwrap();

        assert_eq!(document, document2);
    }
}
