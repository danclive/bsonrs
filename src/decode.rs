use std::{io, error, fmt, string};
use std::io::{Read, Cursor};

use byteorder::{LittleEndian, ReadBytesExt};
use chrono::Utc;
use chrono::offset::{TimeZone, LocalResult};
use serde::de::Deserialize;

use crate::spec::{ElementType, BinarySubtype};
use crate::value::{Value, Array};
use crate::doc::Document;
use crate::serde_impl::decode::Decoder;
use crate::object_id::ObjectId;

const MAX_BSON_SIZE: i32 = 16 * 1024 * 1024;

#[derive(Debug)]
pub enum DecodeError {
    IoError(io::Error),
    FromUtf8Error(string::FromUtf8Error),
    UnrecognizedElementType(u8),
    InvalidArrayKey(usize, String),
    ExpectedField(&'static str),
    UnknownField(String),
    SyntaxError(String),
    EndOfStream,
    InvalidType(String),
    InvalidLength(usize, String),
    DuplicatedField(&'static str),
    UnknownVariant(String),
    InvalidValue(String),
    InvalidTimestamp(i64),
    AmbiguousTimestamp(i64),
    Unknown(String)
}

impl From<io::Error> for DecodeError {
    fn from(err: io::Error) -> DecodeError {
        DecodeError::IoError(err)
    }
}

impl From<string::FromUtf8Error> for DecodeError {
    fn from(err: string::FromUtf8Error) -> DecodeError {
        DecodeError::FromUtf8Error(err)
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DecodeError::IoError(ref inner) => inner.fmt(fmt),
            DecodeError::FromUtf8Error(ref inner) => inner.fmt(fmt),
            DecodeError::UnrecognizedElementType(tag) => {
                write!(fmt, "Unrecognized element type `{}`", tag)
            }
            DecodeError::InvalidArrayKey(ref want, ref got) => {
                write!(fmt, "Invalid array key: expected `{}`, got `{}`", want, got)
            }
            DecodeError::ExpectedField(field_type) => {
                write!(fmt, "Expected a field of type `{}`", field_type)
            }
            DecodeError::UnknownField(ref field) => write!(fmt, "Unknown field `{}`", field),
            DecodeError::SyntaxError(ref inner) => inner.fmt(fmt),
            DecodeError::EndOfStream => write!(fmt, "End of stream"),
            DecodeError::InvalidType(ref desc) => desc.fmt(fmt),
            DecodeError::InvalidLength(ref len, ref desc) => {
                write!(fmt, "Expecting length {}, {}", len, desc)
            }
            DecodeError::DuplicatedField(ref field) => write!(fmt, "Duplicated field `{}`", field),
            DecodeError::UnknownVariant(ref var) => write!(fmt, "Unknown variant `{}`", var),
            DecodeError::InvalidValue(ref desc) => desc.fmt(fmt),
            DecodeError::InvalidTimestamp(ref i) => write!(fmt, "no such local time {}", i),
            DecodeError::AmbiguousTimestamp(ref i) => write!(fmt, "ambiguous local time {}", i),
            DecodeError::Unknown(ref inner) => inner.fmt(fmt),
        }
    }
}

impl error::Error for DecodeError {
    fn description(&self) -> &str {
        match *self {
            DecodeError::IoError(ref inner) => inner.description(),
            DecodeError::FromUtf8Error(ref inner) => inner.description(),
            DecodeError::UnrecognizedElementType(_) => "Unrecognized element type",
            DecodeError::InvalidArrayKey(_, _) => "Invalid array key",
            DecodeError::ExpectedField(_) => "Expected a field",
            DecodeError::UnknownField(_) => "Found an unknown field",
            DecodeError::SyntaxError(ref inner) => inner,
            DecodeError::EndOfStream => "End of stream",
            DecodeError::InvalidType(ref desc) => desc,
            DecodeError::InvalidLength(_, ref desc) => desc,
            DecodeError::DuplicatedField(_) => "Duplicated field",
            DecodeError::UnknownVariant(_) => "Unknown variant",
            DecodeError::InvalidValue(ref desc) => desc,
            DecodeError::InvalidTimestamp(..) => "no such local time",
            DecodeError::AmbiguousTimestamp(..) => "ambiguous local time",
            DecodeError::Unknown(ref inner) => inner,
        }
    }
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            DecodeError::IoError(ref inner) => Some(inner),
            DecodeError::FromUtf8Error(ref inner) => Some(inner),
            _ => None,
        }
    }
}

pub type DecodeResult<T> = Result<T, DecodeError>;

pub(crate) fn read_string(reader: &mut impl Read) -> DecodeResult<String> {
    let len = reader.read_i32::<LittleEndian>()?;

    if len < 1 || len > MAX_BSON_SIZE {
        return Err(DecodeError::InvalidLength(len as usize, format!("invalid length {} for UTF-8 string", len)));
    }

    let mut buf = Vec::with_capacity(len as usize - 1);
    reader.take(len as u64 -1).read_to_end(&mut buf)?;
    let s = String::from_utf8_lossy(&buf).to_string();

    reader.read_u8()?; // The last 0x00

    Ok(s)
}

pub(crate) fn read_cstring(reader: &mut impl Read) -> DecodeResult<String> {
    let mut v = Vec::new();

    loop {
        let c = reader.read_u8()?;
        if c == 0 {
            break;
        }
        v.push(c);
    }

    Ok(String::from_utf8(v)?)
}

#[inline]
pub(crate) fn read_i32(reader: &mut impl Read) -> DecodeResult<i32> {
    reader.read_i32::<LittleEndian>().map_err(From::from)
}

#[inline]
pub(crate) fn read_i64(reader: &mut impl Read) -> DecodeResult<i64> {
    reader.read_i64::<LittleEndian>().map_err(From::from)
}

#[inline]
pub(crate) fn read_u64(reader: &mut impl Read) -> DecodeResult<u64> {
    reader.read_u64::<LittleEndian>().map_err(From::from)
}

fn decode_array(reader: &mut impl Read) -> DecodeResult<Array> {
    let mut arr = Array::new();

    // disregard the length: using Read::take causes infinite type recursion
    read_i32(reader)?;

    loop {
        let tag = reader.read_u8()?;
        if tag == 0 {
            break;
        }

        // check that the key is as expected
        let key = read_cstring(reader)?;
        match key.parse::<usize>() {
            Err(..) => return Err(DecodeError::InvalidArrayKey(arr.len(), key)),
            Ok(idx) => {
                if idx != arr.len() {
                    return Err(DecodeError::InvalidArrayKey(arr.len(), key));
                }
            }
        }

        let val = decode_bson(reader, tag)?;
        arr.push(val)
    }

    Ok(arr)
}

fn decode_bson(reader: &mut impl Read, tag: u8) -> DecodeResult<Value> {
    match ElementType::from(tag) {
        Some(ElementType::Double) => {
            Ok(Value::Double(reader.read_f64::<LittleEndian>()?))
        }
        Some(ElementType::Utf8String) => {
            read_string(reader).map(Value::String)
        }
        Some(ElementType::Document) => {
            decode_document(reader).map(Value::Document)
        }
        Some(ElementType::Array) => {
            decode_array(reader).map(Value::Array)
        }
        Some(ElementType::Binary) => {
            let len = read_i32(reader)?;
            let subtype = BinarySubtype::from(reader.read_u8()?);
            let mut data = Vec::with_capacity(len as usize);
            
            reader.take(len as u64).read_to_end(&mut data)?;
            
            Ok(Value::Binary(subtype, data))
        }
        Some(ElementType::ObjectId) => {
            let mut objid = [0; 12];
            
            for x in &mut objid {
                *x = reader.read_u8()?;
            }
            
            Ok(Value::ObjectId(ObjectId::with_bytes(objid)))
        }
        Some(ElementType::Boolean) => {
            Ok(Value::Boolean(reader.read_u8()? != 0))
        }
        Some(ElementType::NullValue) => {
            Ok(Value::Null)
        }
        Some(ElementType::RegularExpression) => {
            let pat = read_cstring(reader)?;
            let opt = read_cstring(reader)?;
            
            Ok(Value::RegExp(pat, opt))
        }
        Some(ElementType::JavaScriptCode) => {
            read_string(reader).map(Value::JavaScriptCode)
        }
        Some(ElementType::JavaScriptCodeWithScope) => {
            // disregard the length:
            //     using Read::take causes infinite type recursion
            read_i32(reader)?;

            let code = read_string(reader)?;
            let scope = decode_document(reader)?;
            
            Ok(Value::JavaScriptCodeWithScope(code, scope))
        }
        Some(ElementType::Int32) => {
            read_i32(reader).map(Value::Int32)
        }
        Some(ElementType::Int64) => {
            read_i64(reader).map(Value::Int64)
        }
        Some(ElementType::TimeStamp) => {
            read_u64(reader).map(Value::TimeStamp)
        }
        Some(ElementType::UTCDatetime) => {
            let time = read_i64(reader)?;

            let temp_msec = time % 1000;
            let msec = if temp_msec < 0 {
                1000 - temp_msec
            } else {
                temp_msec
            };

            match Utc.timestamp_opt(time / 1000, (msec as u32) * 1_000_000) {
                LocalResult::None => Err(DecodeError::InvalidTimestamp(time)),
                LocalResult::Ambiguous(..) => Err(DecodeError::AmbiguousTimestamp(time)),
                LocalResult::Single(t) => Ok(Value::UTCDatetime(t))
            }
        }
        Some(ElementType::Symbol) => {
            read_string(reader).map(Value::Symbol)
        }
        Some(ElementType::Undefiend) | Some(ElementType::DBPointer) | Some(ElementType::MaxKey) | Some(ElementType::MinKey) | None => {
            Err(DecodeError::UnrecognizedElementType(tag))
        }
    }
}

pub fn decode_document(reader: &mut impl Read) -> DecodeResult<Document> {
    let mut doc = Document::new();

    // disregard the length: using Read::take causes infinite type recursion
    read_i32(reader)?;

    loop {
        let tag = reader.read_u8()?;

        if tag == 0 {
            break;
        }

        let key = read_cstring(reader)?;
        let val = decode_bson(reader, tag)?;

        doc.insert(key, val);
    }

    Ok(doc)
}

pub fn from_bson<'de, T>(value: Value) -> DecodeResult<T>
    where T: Deserialize<'de>
{
    let de = Decoder::new(value);
    Deserialize::deserialize(de)
}

pub fn from_slice<'de, T>(slice: &[u8]) -> DecodeResult<T>
    where T: Deserialize<'de>
{
    let mut reader = Cursor::new(slice);
    let doc = decode_document(&mut reader)?;
    from_bson(Value::Document(doc))
}
