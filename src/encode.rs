use std::io::{self, Write};
use std::fmt;
use std::error;
use std::mem;
use std::i64;

use byteorder::{LittleEndian, WriteBytesExt};
use chrono::Timelike;
use serde::ser::{self, Serialize};

use crate::value::Value;
use crate::serde_impl::encode::Encoder;

#[derive(Debug)]
pub enum EncodeError {
    IoError(io::Error),
    InvalidMapKeyType(Value),
    Unknown(String),
    UnsupportedUnsignedType
}

impl From<io::Error> for EncodeError {
    fn from(err: io::Error) -> EncodeError {
        EncodeError::IoError(err)
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            EncodeError::IoError(ref inner) => inner.fmt(fmt),
            EncodeError::InvalidMapKeyType(ref bson) => {
                write!(fmt, "Invalid map key type: {:?}", bson)
            }
            EncodeError::Unknown(ref inner) => inner.fmt(fmt),
            EncodeError::UnsupportedUnsignedType => write!(fmt, "bson does not support unsigned type"),
        }
    }
}

impl error::Error for EncodeError {
    fn description(&self) -> &str {
        match *self {
            EncodeError::IoError(ref inner) => inner.description(),
            EncodeError::InvalidMapKeyType(_) => "Invalid map key type",
            EncodeError::Unknown(ref inner) => inner,
            EncodeError::UnsupportedUnsignedType => "bson does not support unsigned type",
        }
    }
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            EncodeError::IoError(ref inner) => Some(inner),
            _ => None,
        }
    }
}

impl ser::Error for EncodeError {
    fn custom<T: fmt::Display>(msg: T) -> EncodeError {
        EncodeError::Unknown(msg.to_string())
    }
}

pub type EncodeResult<T> = Result<T, EncodeError>;

pub(crate) fn write_string<W>(writer: &mut W, s: &str) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_i32::<LittleEndian>(s.len() as i32 + 1)?;
    writer.write_all(s.as_bytes())?;
    writer.write_u8(0)?;
    Ok(())
}

pub(crate) fn write_cstring<W>(writer: &mut W, s: &str) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_all(s.as_bytes())?;
    writer.write_u8(0)?;
    Ok(())
}

#[inline]
pub(crate) fn write_i32<W>(writer: &mut W, val: i32) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_i32::<LittleEndian>(val).map_err(From::from)
}

#[inline]
pub(crate) fn write_i64<W>(writer: &mut W, val: i64) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_i64::<LittleEndian>(val).map_err(From::from)
}

#[inline]
pub(crate) fn write_f64<W>(writer: &mut W, val: f64) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_f64::<LittleEndian>(val).map_err(From::from)
}

fn encode_array<W>(writer: &mut W, arr: &[Value]) -> EncodeResult<()>
    where W: Write + ?Sized
{
    // let mut buf = Vec::new();
    // for (key, val) in arr.iter().enumerate() {
    //     encode_bson(&mut buf, &key.to_string(), val)?;
    // }

    // write_i32(
    //     writer,
    //     (buf.len() + mem::size_of::<i32>() + mem::size_of::<u8>()) as i32
    // )?;

    // writer.write_all(&buf)?;
    // writer.write_u8(0)?;
    // Ok(())



    let mut buf = vec![0; mem::size_of::<i32>()];
    for (key, val) in arr.iter().enumerate() {
        encode_bson(&mut buf, &key.to_string(), val)?;
    }

    buf.write_u8(0)?;

    let mut tmp = Vec::new();

    write_i32(&mut tmp, buf.len() as i32)?;

    for i in 0..tmp.len() {
        buf[i] = tmp[i];
    }

    writer.write_all(&buf)?;
    Ok(())
}

pub fn encode_bson<W>(writer: &mut W, key: &str, val: &Value) -> EncodeResult<()>
    where W: Write + ?Sized
{
    writer.write_u8(val.element_type() as u8)?;
    write_cstring(writer, key)?;

    match *val {
        Value::Double(v) => write_f64(writer, v),
        Value::String(ref v) => write_string(writer, &v),
        Value::Array(ref v) => encode_array(writer, &v),
        Value::Document(ref v) => encode_document(writer, v),
        Value::Boolean(v) => writer.write_u8(if v { 0x01 } else { 0x00 }).map_err(From::from),
        Value::RegExp(ref pat, ref opt) => {
            write_cstring(writer, pat)?;
            write_cstring(writer, opt)
        }
        Value::JavaScriptCode(ref code) => write_string(writer, &code),
        Value::ObjectId(ref id) => writer.write_all(&id.bytes()).map_err(From::from),
        Value::JavaScriptCodeWithScope(ref code, ref scope) => {
            let mut buf = Vec::new();
            write_string(&mut buf, code)?;
            encode_document(&mut buf, scope)?;

            write_i32(writer, buf.len() as i32 + 4)?;
            writer.write_all(&buf).map_err(From::from)
        }
        Value::Int32(v) => write_i32(writer, v),
        Value::Int64(v) => write_i64(writer, v),
        Value::TimeStamp(v) => write_i64(writer, v),
        Value::Binary(subtype, ref data) => {
            write_i32(writer, data.len() as i32)?;
            writer.write_u8(From::from(subtype))?;
            writer.write_all(data).map_err(From::from)
        }
        Value::UTCDatetime(ref v) => {
            write_i64(
                writer,
                v.timestamp() * 1000 + i64::from(v.nanosecond() / 1_000_000)
            )
        }
        Value::Null => Ok(()),
        Value::Symbol(ref v) => write_string(writer, &v)
    }
}

pub fn encode_document<'a, S, W, D> (writer: &mut W, document: D) -> EncodeResult<()>
    where S: AsRef<str> + 'a, W: Write + ?Sized, D: IntoIterator<Item = (&'a S, &'a Value)>
{
    let mut buf = vec![0; mem::size_of::<i32>()];
    for (key, val) in document {
        encode_bson(&mut buf, key.as_ref(), val)?;
    }

    buf.write_u8(0)?;

    let mut tmp = Vec::new();

    write_i32(&mut tmp, buf.len() as i32)?;

    for i in 0..tmp.len() {
        buf[i] = tmp[i];
    }

    writer.write_all(&buf)?;
    Ok(())
}

pub fn to_bson<T: ?Sized>(value: &T) -> EncodeResult<Value>
    where T: Serialize
{
    let ser = Encoder::new();
    value.serialize(ser)
}

pub fn to_vec<T: ?Sized>(value: &T) -> EncodeResult<Vec<u8>>
    where T: Serialize
{
    let bson = to_bson(value)?;

    if let Value::Document(object) = bson {
        let mut buf: Vec<u8> = Vec::new();
        encode_document(&mut buf, &object)?;
        return Ok(buf)
    }

    Err(EncodeError::InvalidMapKeyType(bson))
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use crate::encode::encode_document;
    use crate::decode::decode_document;

    #[test]
    fn encode() {
        let document = doc!{"aa": "bb", "cc": [1, 2, 3, 4]};

        let mut buf: Vec<u8> = Vec::new();

        encode_document(&mut buf, &document).unwrap();

        let mut reader = Cursor::new(buf);

        let document2 = decode_document(&mut reader).unwrap();

        assert_eq!(document, document2);
    }
}
