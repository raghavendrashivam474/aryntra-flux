use super::error::Result;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub struct Chunker {
    file: File,
    chunk_size: u32,
    current_index: u32,
    total_chunks: u32,
    bytes_read: u64,
}

impl Chunker {
    pub async fn new(path: &Path, chunk_size: u32) -> Result<Self> {
        let file = File::open(path).await?;
        let meta = file.metadata().await?;
        let file_size = meta.len();
        let total_chunks = file_size.div_ceil(chunk_size as u64) as u32;

        Ok(Self {
            file,
            chunk_size,
            current_index: 0,
            total_chunks,
            bytes_read: 0,
        })
    }

    pub async fn next_chunk(&mut self) -> Result<Option<(u32, Vec<u8>)>> {
        if self.current_index >= self.total_chunks {
            return Ok(None);
        }

        let mut buffer = vec![0u8; self.chunk_size as usize];
        let n = self.file.read(&mut buffer).await?;

        if n == 0 {
            return Ok(None);
        }

        buffer.truncate(n);
        let index = self.current_index;
        self.current_index += 1;
        self.bytes_read += n as u64;

        Ok(Some((index, buffer)))
    }

    pub fn progress(&self) -> (u32, u32) {
        (self.current_index, self.total_chunks)
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}
