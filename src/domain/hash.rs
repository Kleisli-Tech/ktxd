use crate::error::{ProxyError, Result};
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let json_value = serde_json::to_value(value)
        .map_err(|error| ProxyError::Serialization(error.to_string()))?;
    let mut output = Vec::new();
    write_canonical_value(&json_value, &mut output)?;
    Ok(output)
}

pub fn blake3_hash<T: Serialize>(value: &T) -> Result<String> {
    let bytes = canonical_bytes(value)?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

pub fn sha256_hex_of_canonical<T: Serialize>(value: &T) -> Result<String> {
    let bytes = canonical_bytes(value)?;
    let digest = Sha256::digest(&bytes);
    Ok(hex::encode(digest))
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(text) => {
            let encoded = serde_json::to_string(text)
                .map_err(|error| ProxyError::Serialization(error.to_string()))?;
            output.extend_from_slice(encoded.as_bytes());
        }
        Value::Array(items) => {
            output.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(item, output)?;
            }
            output.push(b']');
        }
        Value::Object(object) => write_canonical_object(object, output)?,
    }
    Ok(())
}

fn write_canonical_object(object: &Map<String, Value>, output: &mut Vec<u8>) -> Result<()> {
    output.push(b'{');
    let mut entries: Vec<_> = object.iter().collect();
    entries.sort_by(|left, right| left.0.cmp(right.0));
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        let encoded_key = serde_json::to_string(key)
            .map_err(|error| ProxyError::Serialization(error.to_string()))?;
        output.extend_from_slice(encoded_key.as_bytes());
        output.push(b':');
        write_canonical_value(value, output)?;
    }
    output.push(b'}');
    Ok(())
}
