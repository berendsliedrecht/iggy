use crate::error::Error;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};

pub trait Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error>;
}

pub struct GzCompressor {
    pub min_data_size: u32,
}
impl GzCompressor {
    pub fn new() -> Self {
        Self { min_data_size: 150 }
    }
    pub fn new_with_min_compression_size(min_data_size: u32) -> Self {
        Self { min_data_size }
    }
}

impl Compressor for GzCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let mut decoder = GzDecoder::new(data);
        // can this be handled better (e.g maybe we can allocate a buffer
        // with predetermined size based on compression ratio of specific algorithm
        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: &str = r#"
        (a stench is in 3 5)
         (a pit is nearby)
         (is the wumpus near)
         (Did you go to 3 8)
         (Yes -- Nothing is there)
        (Shoot -- Shoot left)
        (Kill the wumpus -- shoot up)))
        (defun ss (&optional (sentences *sentences*))
        "Run some test sentences, and count how many were¬
        not parsed."
        (count-if-not
        #'(lambda (s)
        (format t "~2&>>> ~(~{~a ~}~)~%"¬
        s)
        (write (second (parse s)) :pretty t))
        *sentences*))"#;

    #[test]
    fn test_gzip_compress() {
        let compressor = GzCompressor::new();
        let result = compressor.compress(DATA.as_bytes());
        assert!(result.is_ok());
        let compressed = result.unwrap();
        assert_ne!(compressed.len(), DATA.len());
    }

    #[test]
    fn test_gzip_decompress() {
        let compressor = GzCompressor::new();
        let compressed_data = compressor.compress(DATA.as_bytes()).unwrap();
        let result = compressor.decompress(&compressed_data);

        assert!(result.is_ok());
        let decompressed = result.unwrap();
        assert_eq!(decompressed, DATA.as_bytes());
    }
}
