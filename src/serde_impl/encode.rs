use std::{u32, i32, f64};

use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeTuple, SerializeTupleStruct,
                 SerializeTupleVariant, SerializeMap, SerializeStruct, SerializeStructVariant};

use crate::doc::Document;
use crate::value::{Value, Array, UTCDateTime, TimeStamp};
use crate::encode::to_bson;
use crate::encode::EncodeError;
use crate::encode::EncodeResult;
use crate::spec::BinarySubtype;

impl Serialize for Document {
     #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self {
            map.serialize_key(k)?;
            map.serialize_value(v)?;
        }
        map.end()
    }
}

impl Serialize for Value {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            Value::Double(v) => serializer.serialize_f64(v),
            Value::String(ref v) => serializer.serialize_str(v),
            Value::Array(ref v) => v.serialize(serializer),
            Value::Document(ref v) => v.serialize(serializer),
            Value::Boolean(v) => serializer.serialize_bool(v),
            Value::Null => serializer.serialize_unit(),
            Value::Int32(v) => serializer.serialize_i32(v),
            Value::Int64(v) => serializer.serialize_i64(v),
            _ => {
                let doc = self.to_extended_document();
                doc.serialize(serializer)
            }
        }
    }
}

#[derive(Default)]
pub struct Encoder;

impl Encoder {
    pub fn new() -> Encoder {
        Encoder
    }
}

impl Serializer for Encoder {
    type Ok = Value;
    type Error = EncodeError;

    type SerializeSeq = ArraySerializer;
    type SerializeTuple = TupleSerializer;
    type SerializeTupleStruct = TupleStructSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructVariantSerializer;

    #[inline]
    fn serialize_bool(self, value: bool) -> EncodeResult<Value> {
        Ok(Value::Boolean(value))
    }

    #[inline]
    fn serialize_i8(self, value: i8) -> EncodeResult<Value> {
        self.serialize_i32(i32::from(value))
    }

    #[inline]
    fn serialize_u8(self, _value: u8) -> EncodeResult<Value> {
        Err(EncodeError::UnsupportedUnsignedType)
    }

    #[inline]
    fn serialize_i16(self, value: i16) -> EncodeResult<Value> {
        self.serialize_i32(i32::from(value))
    }

    #[inline]
    fn serialize_u16(self, _value: u16) -> EncodeResult<Value> {
        Err(EncodeError::UnsupportedUnsignedType)
    }

    #[inline]
    fn serialize_i32(self, value: i32) -> EncodeResult<Value> {
        Ok(Value::Int32(value))
    }

    #[inline]
    fn serialize_u32(self, _value: u32) -> EncodeResult<Value> {
        Err(EncodeError::UnsupportedUnsignedType)
    }

    #[inline]
    fn serialize_i64(self, value: i64) -> EncodeResult<Value> {
        Ok(Value::Int64(value))
    }

    #[inline]
    fn serialize_u64(self, _value: u64) -> EncodeResult<Value> {
        Err(EncodeError::UnsupportedUnsignedType)
    }

    #[inline]
    fn serialize_f32(self, value: f32) -> EncodeResult<Value> {
        self.serialize_f64(f64::from(value))
    }

    #[inline]
    fn serialize_f64(self, value: f64) -> EncodeResult<Value> {
        Ok(Value::Double(value))
    }

    #[inline]
    fn serialize_char(self, value: char) -> EncodeResult<Value> {
        let mut s = String::new();
        s.push(value);
        self.serialize_str(&s)
    }

    #[inline]
    fn serialize_str(self, value: &str) -> EncodeResult<Value> {
        Ok(Value::String(value.to_string()))
    }

    fn serialize_bytes(self, value: &[u8]) -> EncodeResult<Value> {
        Ok(Value::Binary(BinarySubtype::Generic, value.into()))
    }

    #[inline]
    fn serialize_none(self) -> EncodeResult<Value> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_some<V: ?Sized>(self, value: &V) -> EncodeResult<Value>
        where V: Serialize
    {
        value.serialize(self)
    }

    #[inline]
    fn serialize_unit(self) -> EncodeResult<Value> {
        Ok(Value::Null)
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> EncodeResult<Value> {
        self.serialize_unit()
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str
    ) -> EncodeResult<Value> {
        Ok(Value::String(variant.to_string()))
    }

    #[inline]
    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> EncodeResult<Value>
        where T: Serialize
    {
        value.serialize(self)
    }

    #[inline]
    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T
    ) -> EncodeResult<Value>
        where T: Serialize
    {
        let mut newtype_variant = Document::new();
        newtype_variant.insert(variant, to_bson(value)?);
        Ok(newtype_variant.into())
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> EncodeResult<Self::SerializeSeq> {
        Ok(ArraySerializer { inner: Array::with_capacity(len.unwrap_or(0)) })
    }

    #[inline]
    fn serialize_tuple(self, len: usize) -> EncodeResult<Self::SerializeTuple> {
        Ok(TupleSerializer { inner: Array::with_capacity(len) })
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize
    ) -> EncodeResult<Self::SerializeTupleStruct> {
        Ok(TupleStructSerializer { inner: Array::with_capacity(len) })
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize
    ) -> EncodeResult<Self::SerializeTupleVariant> {
        Ok(TupleVariantSerializer {
            inner: Array::with_capacity(len),
            name: variant,
        })
    }

    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> EncodeResult<Self::SerializeMap> {
        Ok(MapSerializer {
            inner: Document::new(),
            next_key: None,
        })
    }

    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> EncodeResult<Self::SerializeStruct> {
        Ok(StructSerializer { inner: Document::new() })
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize
    ) -> EncodeResult<Self::SerializeStructVariant> {
        Ok(StructVariantSerializer {
            name: variant,
            inner: Document::new(),
        })
    }
}


pub struct ArraySerializer {
    inner: Array
}

impl SerializeSeq for ArraySerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> EncodeResult<()> {
        self.inner.push(to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        Ok(Value::Array(self.inner))
    }
}

pub struct TupleSerializer {
    inner: Array
}

impl SerializeTuple for TupleSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> EncodeResult<()> {
        self.inner.push(to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        Ok(Value::Array(self.inner))
    }
}

pub struct TupleStructSerializer {
    inner: Array
}

impl SerializeTupleStruct for TupleStructSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> EncodeResult<()> {
        self.inner.push(to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        Ok(Value::Array(self.inner))
    }
}

pub struct TupleVariantSerializer {
    inner: Array,
    name: &'static str
}

impl SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> EncodeResult<()> {
        self.inner.push(to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        let mut tuple_variant = Document::new();
        tuple_variant.insert(self.name, self.inner);
        Ok(tuple_variant.into())
    }
}

pub struct MapSerializer {
    inner: Document,
    next_key: Option<String>
}

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> EncodeResult<()> {
        self.next_key = match to_bson(&key)? {
            Value::String(s) => Some(s),
            other => return Err(EncodeError::InvalidMapKeyType(other)),
        };
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> EncodeResult<()> {
        let key = self.next_key.take().unwrap_or_else(|| "".to_string());
        self.inner.insert(key, to_bson(&value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        Ok(Value::from_extended_document(self.inner))
    }
}

pub struct StructSerializer {
    inner: Document
}

impl SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> EncodeResult<()> {
        self.inner.insert(key, to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        Ok(Value::from_extended_document(self.inner))
    }
}

pub struct StructVariantSerializer {
    inner: Document,
    name: &'static str
}

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> EncodeResult<()> {
        self.inner.insert(key, to_bson(value)?);
        Ok(())
    }

    fn end(self) -> EncodeResult<Value> {
        let var = Value::from_extended_document(self.inner);

        let mut struct_variant = Document::new();
        struct_variant.insert(self.name, var);

        Ok(Value::Document(struct_variant))
    }
}

impl Serialize for UTCDateTime {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        // Cloning a `DateTime` is extremely cheap
        let document = Value::UTCDatetime(self.0);
        document.serialize(serializer)
    }
}

impl Serialize for TimeStamp {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let ts = ((self.t.to_le() as u64) << 32) | (self.i.to_le() as u64);
        let doc = Value::TimeStamp(ts as i64);
        doc.serialize(serializer)
    }
}
