pub use value::{Value, Array};
pub use doc::Document;
pub use object_id::ObjectId;

mod macros;
pub mod value;
pub mod doc;
pub mod encode;
pub mod decode;
pub mod serde_impl;
mod spec;
mod util;
pub mod object_id;

#[cfg(test)]
mod test {
	use serde_derive::{Serialize, Deserialize};
	use serde_bytes;
	use crate::encode::to_bson;
	use crate::decode::from_bson;
	use crate::Value;
	use crate::doc;

	#[derive(Serialize, Deserialize, Debug, PartialEq)]
	pub struct Foo {
		b: i64,
		c: f64,
		d: String,
		#[serde(with = "serde_bytes")]
		e: Vec<u8>
	}

	#[test]
	fn serialize_and_deserialize() {
		let foo = Foo {
			b: 2,
			c: 3.0,
			d: "4".to_string(),
			e: vec![1, 2, 3, 4]
		};

		let bson = to_bson(&foo).unwrap();
		let foo2: Foo = from_bson(bson).unwrap();

		assert_eq!(foo, foo2);
	}

	#[test]
	fn into_and_from_json() {
		let foo = Foo {
			b: 2,
			c: 3.0,
			d: "4".to_string(),
			e: vec![1, 2, 3, 4]
		};

		let bson = to_bson(&foo).unwrap();

		let json = bson.to_json();

		let bson2 = Value::from_json(json);

		assert_eq!(bson, bson2);
	}

	#[test]
	fn binary() {
		let byte = vec![1u8, 2, 3, 4];
		let doc = doc!{"aa": "bb", "byte": byte.clone()};
		let byte2 = doc.get_binary("byte").unwrap();

		assert_eq!(&byte, byte2);

		let mut doc2 = doc!{"aa": "bb"};
		doc2.insert("byte", byte);

		assert_eq!(doc, doc2);
	}
}
