use std::result;
use std::fmt;
use std::io::{Write, Read, Cursor};
use std::mem;
use std::iter::{FromIterator, Extend};

use linked_hash_map::LinkedHashMap;
use chrono::{DateTime, Utc};
use byteorder::WriteBytesExt;

use crate::value::{Value, Array};
use crate::encode::{encode_document, encode_bson, write_i32, EncodeResult};
use crate::decode::{decode_document, DecodeResult};
use crate::spec::BinarySubtype;
use crate::object_id::ObjectId;

pub use linked_hash_map::{IntoIter, Iter, IterMut};

#[derive(PartialEq, Debug)]
pub enum Error {
    NotPresent,
    UnexpectedType,
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Clone, PartialEq, Eq, Default)]
pub struct Document {
    inner: LinkedHashMap<String, Value>
}

impl Document {
    pub fn new() -> Document {
        Document {
            inner: LinkedHashMap::new()
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.inner.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.inner.get_mut(key)
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

    pub fn insert_value(&mut self, key: String, value: Value) -> Option<Value> {
        self.inner.insert(key, value)
    }

    pub fn insert<K: Into<String>, V: Into<Value>>(&mut self, key: K, value: V) -> Option<Value> {
        self.insert_value(key.into(), value.into())
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> Iter<'_, String, Value> {
        self.into_iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, String, Value> {
        self.into_iter()
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

    pub fn get_time_stamp(&self, key: &str) -> Result<i64> {
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

    pub fn encode(&self, writer: &mut (impl Write + ?Sized)) -> EncodeResult<()> {
        encode_document(writer, self)
    }

    pub fn decode(reader: &mut (impl Read + ?Sized)) -> DecodeResult<Document> {
        decode_document(reader)
    }

    pub fn to_vec(&self) -> EncodeResult<Vec<u8>> {
        let mut buf = vec![0; mem::size_of::<i32>()];
        for (key, val) in self {
            encode_bson(&mut buf, key.as_ref(), val)?;
        }

        buf.write_u8(0)?;

        let mut tmp = Vec::new();

        write_i32(&mut tmp, buf.len() as i32)?;

        for i in 0..tmp.len() {
            buf[i] = tmp[i];
        }

        Ok(buf)
    }

    pub fn from_slice(slice: &[u8]) -> DecodeResult<Document> {
        let mut reader = Cursor::new(slice);
        decode_document(&mut reader)
    }

    pub fn extend<I: Into<Document>>(&mut self, iter: I) {
        self.inner.extend(iter.into());
    }

    pub fn front(&self) -> Option<(&String, &Value)> {
        self.inner.front()
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
        let mut document = Document::new();

        for (k, v) in iter {
            document.insert(k, v);
        }

        document
    }
}

impl From<LinkedHashMap<String, Value>> for Document {
    fn from(map: LinkedHashMap<String, Value>) -> Document {
        Document { inner: map }
    }
}

#[cfg(test)]
mod test {
    use crate::Document;

    #[test]
    fn to_vec() {
        let document = doc!{"aa": "bb"};

        let vec = document.to_vec().unwrap();

        let document2 = Document::from_slice(&vec).unwrap();

        assert_eq!(document, document2);
    }
}
