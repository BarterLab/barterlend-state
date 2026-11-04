//! Streaming encoders preserve the existing JSON bytes/checksums without full digest copies.
use std::io::{self, Write};
use alloy_primitives::{B256, Keccak256};
use serde::Serialize;
use crate::{Error, Result};

struct Digest(Keccak256);
impl Write for Digest {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> { self.0.update(bytes); Ok(bytes.len()) }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
pub(crate) fn digest(value: &impl Serialize) -> Result<B256> {
    let mut writer = io::BufWriter::with_capacity(8192, Digest(Keccak256::new()));
    serde_json::to_writer(&mut writer, value)?;
    let writer = writer.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    Ok(writer.0.finalize())
}
struct Limited { bytes: Vec<u8>, cap: usize, exceeded: bool }
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = self.cap.saturating_sub(self.bytes.len());
        if bytes.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::other("checkpoint exceeds max_bytes"));
        }
        let needed = self.bytes.len() + bytes.len();
        if needed > self.bytes.capacity() {
            let capacity = needed.max(self.bytes.capacity().saturating_mul(2)).min(self.cap);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes); Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
pub(crate) fn encode(value: &impl Serialize, cap: usize) -> Result<Vec<u8>> {
    let mut writer = Limited { bytes: Vec::with_capacity(cap.min(4096)), cap, exceeded: false };
    let result = serde_json::to_writer(&mut writer, value);
    if writer.exceeded { return Err(Error::ResourceLimit("checkpoint exceeds max_bytes".into())); }
    result?;
    Ok(writer.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_bytes_hash_and_boundary_are_legacy_compatible() {
        for value in [serde_json::json!({"unicode":"Привет 🦀","raw":"18446744073709551617","x":[null,false,0]}),
            serde_json::json!(["x".repeat(100_000),"\\\n\"",u64::MAX])] {
            let old = serde_json::to_vec(&value).unwrap();
            assert_eq!(digest(&value).unwrap(), alloy_primitives::keccak256(&old));
            assert_eq!(encode(&value, old.len()).unwrap(), old);
            assert!(matches!(encode(&value, old.len()-1), Err(Error::ResourceLimit(_))));
        }
    }
    #[test]
    fn oversized_serialization_stops_before_visiting_remaining_items() {
        struct Many(std::cell::Cell<usize>);
        impl Serialize for Many {
            fn serialize<S:serde::Serializer>(&self, s:S)->std::result::Result<S::Ok,S::Error> {
                use serde::ser::SerializeSeq;
                let mut seq=s.serialize_seq(Some(1_000_000))?;
                for _ in 0..1_000_000 { self.0.set(self.0.get()+1); seq.serialize_element(&"x".repeat(1024))?; }
                seq.end()
            }
        }
        let value=Many(std::cell::Cell::new(0));
        assert!(matches!(encode(&value,4096),Err(Error::ResourceLimit(_))));
        assert!(value.0.get()<10);
        let mut writer=Limited{bytes:Vec::new(),cap:4096,exceeded:false};
        for _ in 0..4096 {writer.write_all(b"x").unwrap();assert!(writer.bytes.capacity()<=4096);}
        assert!(writer.write_all(b"x").is_err());
    }
}
