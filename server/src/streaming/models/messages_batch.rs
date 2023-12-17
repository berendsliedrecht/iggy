use bytes::Bytes;

#[derive(Debug)]
pub struct MessagesBatch {
    pub base_offset: u64,
    pub length: u32,
    pub last_offset_delta: u32,
    pub messages: Bytes,
}
