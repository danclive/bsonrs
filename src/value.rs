use std::fmt;
use std::ops::{Deref, DerefMut};
use std::{f64, i64};
use std::iter::FromIterator;

use chrono::{DateTime, Utc, Timelike};
use chrono::offset::TimeZone;
use serde_json;
use serde_json::json;

use crate::doc::Document;
use crate::spec::{ElementType, BinarySubtype};
use crate::util::hex::{ToHex, FromHex};
use crate::object_id::ObjectId;
use crate::doc;

#[derive(Clone, PartialEq)]
pub enum Value {
    Double(f64),
    String(String),
    Array(Array),
    Document(Document),
    Boolean(bool),
    Null,
    RegExp(String, String),
    JavaScriptCode(String),
    JavaScriptCodeWithScope(String, Document),
    Int32(i32),
    Int64(i64),
    TimeStamp(i64),
    Binary(BinarySubtype, Vec<u8>),
    ObjectId(ObjectId),
    UTCDatetime(DateTime<Utc>),
    Symbol(String)
}

impl Eq for Value {}

#[derive(Clone, PartialEq)]
pub struct Array {
    inner: Vec<Value>
}

impl fmt::Debug for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Double(p) => write!(fmt, "Double({:?})", p),
            Value::String(ref s) => write!(fmt, "String({})", s),
            Value::Array(ref vec) => write!(fmt, "Array({:?})", vec),
            Value::Document(ref doc) => write!(fmt, "{:?}", doc),
            Value::Boolean(b) => write!(fmt, "Boolean({:?})", b),
            Value::Null => write!(fmt, "Null"),
            Value::RegExp(ref pat, ref opt) => write!(fmt, "RegExp(/{:?}/{:?})", pat, opt),
            Value::JavaScriptCode(ref s) => write!(fmt, "JavaScriptCode({:?})", s),
            Value::JavaScriptCodeWithScope(ref s, ref scope) => {
                write!(fmt, "JavaScriptCodeWithScope({:?}, {:?})", s, scope)
            }
            Value::Int32(v) => write!(fmt, "Int32({:?})", v),
            Value::Int64(v) => write!(fmt, "Int64({:?})", v),
            Value::TimeStamp(i) => {
                let time = (i >> 32) as i32;
                let inc = (i & 0xFFFF_FFFF) as i32;

                write!(fmt, "TimeStamp({}, {})", time, inc)
            }
            Value::Binary(t, ref vec) => write!(fmt, "BinData({}, 0x{})", u8::from(t), vec.to_hex()),
            Value::ObjectId(ref id) => write!(fmt, "ObjectId({})", id),
            Value::UTCDatetime(date_time) => write!(fmt, "UTCDatetime({:?})", date_time),
            Value::Symbol(ref sym) => write!(fmt, "Symbol({:?})", sym)
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Double(f) => write!(fmt, "{}", f),
            Value::String(ref s) => write!(fmt, "\"{}\"", s),
            Value::Array(ref vec) => {
                write!(fmt, "[")?;

                let mut first = true;
                for bson in vec.iter() {
                    if !first {
                        write!(fmt, ", ")?;
                    }

                    write!(fmt, "{}", bson)?;
                    first = false;
                }

                write!(fmt, "]")
            }
            Value::Document(ref doc) => write!(fmt, "{}", doc),
            Value::Boolean(b) => write!(fmt, "{}", b),
            Value::Null => write!(fmt, "null"),
            Value::RegExp(ref pat, ref opt) => write!(fmt, "/{}/{}", pat, opt),
            Value::JavaScriptCode(ref s) |
            Value::JavaScriptCodeWithScope(ref s, _) => fmt.write_str(&s),
            Value::Int32(i) => write!(fmt, "{}", i),
            Value::Int64(i) => write!(fmt, "{}", i),
            Value::TimeStamp(i) => {
                let time = (i >> 32) as i32;
                let inc = (i & 0xFFFF_FFFF) as i32;

                write!(fmt, "Timestamp({}, {})", time, inc)
            }
            Value::Binary(t, ref vec) => {
                write!(fmt, "BinData({}, 0x{})", u8::from(t), vec.to_hex())
            }
            Value::ObjectId(ref id) => write!(fmt, "ObjectId(\"{}\")", id),
            Value::UTCDatetime(date_time) => write!(fmt, "Date(\"{}\")", date_time),
            Value::Symbol(ref sym) => write!(fmt, "Symbol(\"{}\")", sym)
        }
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Value {
        Value::Double(f64::from(f))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Value {
        Value::Double(f)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Value {
        Value::Int32(i)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Value {
        Value::Int64(i)
    }
}

impl From<u32> for Value {
    fn from(a: u32) -> Value {
        Value::Int32(a as i32)
    }
}

impl From<u64> for Value {
    fn from(a: u64) -> Value {
        Value::Int64(a as i64)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &str) -> Value {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::String(s)
    }
}

impl<'a> From<&'a String> for Value {
    fn from(s: &'a String) -> Value {
        Value::String(s.to_owned())
    }
}

impl From<Array> for Value {
    fn from(a: Array) -> Value {
        Value::Array(a)
    }
}

impl From<Document> for Value {
    fn from(d: Document) -> Value {
        Value::Document(d)
    }
}

impl From<Vec<Document>> for Value {
    fn from(v: Vec<Document>) -> Value {
        Value::Array(v.into())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Boolean(b)
    }
}

impl From<(String, String)> for Value {
    fn from((r1, r2): (String, String)) -> Value {
        Value::RegExp(r1, r2)
    }
}

impl From<(BinarySubtype, Vec<u8>)> for Value {
    fn from((b1, b2): (BinarySubtype, Vec<u8>)) -> Value {
        Value::Binary(b1, b2)
    }
}

impl From<Vec<u8>> for Value {
    fn from(vec: Vec<u8>) -> Value {
        Value::Binary(BinarySubtype::Generic, vec)
    }
}

impl From<[u8; 12]> for Value {
    fn from(o: [u8; 12]) -> Value {
        Value::ObjectId(ObjectId::with_bytes(o))
    }
}

impl From<ObjectId> for Value {
    fn from(o: ObjectId) -> Value {
        Value::ObjectId(o)
    }
}

impl From<DateTime<Utc>> for Value {
    fn from(d: DateTime<Utc>) -> Value {
        Value::UTCDatetime(d)
    }
}

impl From<Vec<Vec<u8>>> for Value {
    fn from(vec: Vec<Vec<u8>>) -> Value {
        let array: Array = vec.into_iter().map(|v| v.into()).collect();
        Value::Array(array)
    }
}

impl Value {
    pub fn element_type(&self) -> ElementType {
        match *self {
            Value::Double(..) => ElementType::Double,
            Value::String(..) => ElementType::Utf8String,
            Value::Array(..) => ElementType::Array,
            Value::Document(..) => ElementType::Document,
            Value::Boolean(..) => ElementType::Boolean,
            Value::Null => ElementType::NullValue,
            Value::RegExp(..) => ElementType::RegularExpression,
            Value::JavaScriptCode(..) => ElementType::JavaScriptCode,
            Value::JavaScriptCodeWithScope(..) => ElementType::JavaScriptCodeWithScope,
            Value::Int32(..) => ElementType::Int32,
            Value::Int64(..) => ElementType::Int64,
            Value::TimeStamp(..) => ElementType::TimeStamp,
            Value::Binary(..) => ElementType::Binary,
            Value::ObjectId(..) => ElementType::ObjectId,
            Value::UTCDatetime(..) => ElementType::UTCDatetime,
            Value::Symbol(..) => ElementType::Symbol
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Value::Double(ref v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match *self {
            Value::String(ref s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Array> {
        match *self {
            Value::Array(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_document(&self) -> Option<&Document> {
        match *self {
            Value::Document(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Value::Boolean(ref v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match *self {
            Value::Int32(ref v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Value::Int64(ref v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_object_id(&self) -> Option<&ObjectId> {
        match *self {
            Value::ObjectId(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_utc_date_time(&self) -> Option<&DateTime<Utc>> {
        match *self {
            Value::UTCDatetime(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_symbol(&self) -> Option<&str> {
        match *self {
            Value::Symbol(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<i64> {
        match *self {
            Value::TimeStamp(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match *self {
            Value::Null => Some(()),
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        self.clone().into()
    }

    pub fn into_json(self) -> serde_json::Value {
        self.into()
    }

    pub fn from_json(val: serde_json::Value) -> Value {
        val.into()
    }

    pub fn to_extended_document(&self) -> Document {
        match *self {
            Value::RegExp(ref pat, ref opt) => {
                doc!{
                    "$regex": pat.clone(),
                    "$options": opt.clone()
                }
            }
            Value::JavaScriptCode(ref code) => {
                doc!{
                    "$code": code.clone()
                }
            }
            Value::JavaScriptCodeWithScope(ref code, ref scope) => {
                doc!{
                    "$code": code.clone(),
                    "$scope": scope.clone()
                }
            }
            Value::TimeStamp(v) => {
                let time = (v >> 32) as i32;
                let inc = (v & 0xFFFF_FFFF) as i32;

                doc!{
                    "t": time,
                    "i": inc
                }
            }
            Value::Binary(t, ref v) => {
                let tval: u8 = From::from(t);
                doc!{
                    "$binary": v.to_hex(),
                    "type": i64::from(tval)
                }
            }
            Value::ObjectId(ref v) => {
                doc!{
                    "$oid": v.to_string()
                }
            }
            Value::UTCDatetime(ref v) => {
                doc!{
                    "$date": {
                        "$numberLong": v.timestamp() * 1000 + i64::from(v.nanosecond()) / 1_000_000
                    }
                }
            }
            Value::Symbol(ref v) => {
                doc!{
                    "$symbol": v.to_owned()
                }
            }
            _ => panic!("Attempted conversion of invalid data type: {}", self)
        }
    }

    pub fn from_extended_document(values: Document) -> Value {
        if values.len() == 2 {
            if let (Ok(pat), Ok(opt)) = (values.get_str("$regex"), values.get_str("$options")) {
                return Value::RegExp(pat.to_owned(), opt.to_owned());

            } else if let (Ok(code), Ok(scope)) =
                (values.get_str("$code"), values.get_document("$scope")) {
                return Value::JavaScriptCodeWithScope(code.to_owned(), scope.clone());

            } else if let (Ok(t), Ok(i)) = (values.get_i32("t"), values.get_i32("i")) {
                let timestamp = (i64::from(t) << 32) + i64::from(i);
                return Value::TimeStamp(timestamp);

            } else if let (Ok(t), Ok(i)) = (values.get_i64("t"), values.get_i64("i")) {
                let timestamp = (t << 32) + i;
                return Value::TimeStamp(timestamp);

            } else if let (Ok(hex), Ok(t)) = (values.get_str("$binary"), values.get_i64("type")) {
                let ttype = t as u8;
                return Value::Binary(From::from(ttype), FromHex::from_hex(hex.as_bytes()).unwrap());
            }

        } else if values.len() == 1 {
            if let Ok(code) = values.get_str("$code") {
                return Value::JavaScriptCode(code.to_string());

            } else if let Ok(hex) = values.get_str("$oid") {
                return Value::ObjectId(ObjectId::with_string(hex).unwrap());

            } else if let Ok(long) = values.get_document("$date").and_then(|inner| inner.get_i64("$numberLong")) {
                return Value::UTCDatetime(Utc.timestamp(long / 1000, ((long % 1000) * 1_000_000) as u32));
            } else if let Ok(sym) = values.get_str("$symbol") {
                return Value::Symbol(sym.to_string());
            }
        }

        Value::Document(values)
    }
}

impl From<serde_json::Value> for Value {
    fn from(a: serde_json::Value) -> Value {
        match a {
            serde_json::Value::Number(x) => {
                x.as_i64().map(Value::from)
                    .or_else(|| x.as_u64().map(Value::from))
                    .or_else(|| x.as_f64().map(Value::from))
                    .unwrap_or_else(|| panic!("Invalid number value: {}", x))
            }
            serde_json::Value::String(x) => x.into(),
            serde_json::Value::Bool(x) => x.into(),
            serde_json::Value::Array(x) => Value::Array(x.into_iter().map(Value::from).collect()),
            serde_json::Value::Object(x) => {
                Value::from_extended_document(
                    x.into_iter().map(|(k, v)| (k.clone(), v.into())).collect()
                )
            }
            serde_json::Value::Null => Value::Null,
        }
    }
}

impl Into<serde_json::Value> for Value {
    fn into(self) -> serde_json::Value {
        match self {
            Value::Double(v) => json!(v),
            Value::String(v) => json!(v),
            Value::Array(v) => json!(v.into_inner()),
            Value::Document(v) => json!(v),
            Value::Boolean(v) => json!(v),
            Value::Null => serde_json::Value::Null,
            Value::RegExp(pat, opt) => {
                json!({
                    "$regex": pat,
                    "$options": opt
                })
            }
            Value::JavaScriptCode(code) => json!({"$code": code}),
            Value::JavaScriptCodeWithScope(code, scope) => {
                json!({
                    "$code": code,
                    "scope": scope
                })
            }
            Value::Int32(v) => v.into(),
            Value::Int64(v) => v.into(),
            Value::TimeStamp(v) => {
                let time = v >> 32;
                let inc = v & 0x0000_FFFF;
                json!({
                    "t": time,
                    "i": inc
                })
            }
            Value::Binary(t, ref v) => {
                let tval: u8 = From::from(t);
                json!({
                    "type": tval,
                    "$binary": v.to_hex()
                })
            }
            Value::ObjectId(v) => json!({"$oid": v.to_string()}),
            Value::UTCDatetime(v) => {
                json!({
                    "$date": {
                        "$numberLong": (v.timestamp() * 1000) + i64::from(v.nanosecond() / 1_000_000)
                    }
                })
            }
            // FIXME: Don't know what is the best way to encode Symbol type
            Value::Symbol(v) => json!({"$symbol": v}),
        }
    }
}

impl Array {
    pub fn new() -> Array {
        Array {
            inner: Vec::new()
        }
    }

    pub fn with_capacity(capacity: usize) -> Array {
        Array {
            inner: Vec::with_capacity(capacity)
        }
    }

    pub fn from_vec(vec: Vec<Value>) -> Array {
        Array {
            inner: vec
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn push(&mut self, value: Value) {
        self.inner.push(value);
    }

    pub fn inner(&self) -> &Vec<Value> {
        &self.inner
    }

    pub fn into_mut(&mut self) -> &mut Vec<Value> {
        &mut self.inner
    }

    pub fn into_inner(self) -> Vec<Value> {
        self.inner
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.into_iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Value> {
        self.into_iter()
    }
}

impl fmt::Debug for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Deref for Array {
    type Target = Vec<Value>;
    fn deref(&self) -> &Vec<Value> {
        &self.inner
    }
}

impl DerefMut for Array {
    fn deref_mut(&mut self) -> &mut Vec<Value> {
        &mut self.inner
    }
}

macro_rules! from_impls {
    ($($T:ty)+) => {
        $(
            impl From<Vec<$T>> for Array {
                fn from(vec: Vec<$T>) -> Array {
                    vec.into_iter().map(|v| v.into()).collect()
                }
            }
        )+
    }
}

from_impls! {
    f32 f64 i32 i64 &str String &String Array
    Document bool DateTime<Utc> Vec<u8>
}

impl IntoIterator for Array {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a Array {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a> IntoIterator for &'a mut Array {
    type Item = &'a mut Value;
    type IntoIter = std::slice::IterMut<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl FromIterator<Value> for Array {
    fn from_iter<I: IntoIterator<Item=Value>>(iter: I) -> Self {
        let mut array = Array::new();

        for i in iter {
            array.push(i);
        }

        array
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Copy, Clone)]
pub struct UTCDateTime(pub DateTime<Utc>);

impl Deref for UTCDateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UTCDateTime {
    fn deref_mut(&mut self) -> &mut DateTime<Utc> {
        &mut self.0
    }
}

impl Into<DateTime<Utc>> for UTCDateTime {
    fn into(self) -> DateTime<Utc> {
        self.0
    }
}

impl From<DateTime<Utc>> for UTCDateTime {
    fn from(x: DateTime<Utc>) -> Self {
        UTCDateTime(x)
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Copy, Clone)]
pub struct TimeStamp {
    pub t: u32,
    pub i: u32,
}
