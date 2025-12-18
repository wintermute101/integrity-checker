use serde::{Serialize, Deserialize};
use postcard::{from_bytes, to_allocvec};
use std::time::UNIX_EPOCH;
use chrono::DateTime;
use redb::{Value,Key};
use std::time::Duration;

#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;

use super::error::IntegrityWatcherError;

#[derive(Debug, Clone)]
pub struct Bandwidth{
    bytes: u64,
    duration: Duration,
}

impl Bandwidth {
    fn new(size: ByteSize, duration: Duration) -> Self{
        Bandwidth { bytes: size.into(), duration }
    }
}

impl std::fmt::Display for Bandwidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let bandwidth = self.bytes as f64 / self.duration.as_secs_f64();
        let mut pow = (bandwidth.log2() / 10.0).round() as u32;
        let postfix = match pow{
            0 => "B/s",
            1 => "KiB/s",
            2 => "MiB/s",
            3 => "GiB/s",
            4 => "TiB/s",
            5 => "PiB/s",
            _ => { pow = 5; "PiB/s"},
        };

        let b = bandwidth / (1_u64 << (pow*10)) as f64;
        f.write_fmt(format_args!("{b:.2}{postfix}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ByteSize{
    size: u64,
}

impl ByteSize {
    pub fn new(size: u64) -> Self{
        ByteSize { size }
    }

    pub fn add_size(&mut self, size: &Self) {
        self.size += size.size;
    }

    pub fn bandwidth(&self, duration: Duration) -> Bandwidth{
        Bandwidth::new(*self, duration)
    }
}

impl Default for ByteSize {
    fn default() -> Self {
        ByteSize { size: 0 }
    }
}

impl Into<u64> for ByteSize {
    fn into(self) -> u64 {
        self.size
    }
}

impl From<u64> for ByteSize {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for ByteSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut pow = 0;
        let mut size = self.size;
        loop {
            size >>= 10;
            if size == 0{
                break;
            }
            pow += 1;
        }
        let postfix = match pow{
            0 => "B",
            1 => "KiB",
            2 => "MiB",
            3 => "GiB",
            4 => "TiB",
            5 => "PiB",
            _ => { pow = 5; "PiB"},
        };

        if pow == 0{
            f.write_fmt(format_args!("{}{postfix}", self.size))
        }
        else{
            let b = self.size as f64 / (1_u64 << (pow*10)) as f64;
            f.write_fmt(format_args!("{b:.2}{postfix}"))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Hash{
    hash: [u8;32],
}

impl From<[u8;32]> for Hash {
    fn from(value: [u8;32]) -> Self {
        Hash { hash: value }
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in self.hash{
            write!(f, "{:02x}", i)?;
        }
        Ok(())
    }
}

impl Value for Hash {
    type SelfType<'a> = Self;
    type AsBytes<'a> = &'a[u8;32];

    fn fixed_width() -> Option<usize> {
        Some(32)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where Self: 'a{
        from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        &value.hash
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileMetadata")
    }
}

impl Key for Hash {
   fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
       data1.cmp(data2)
   }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
pub struct SymlinkMetadata{
    pub data: String,
    pub permissions: u32,
    pub modified: u64,
    pub size: ByteSize,
}

impl SymlinkMetadata {
    pub fn new(meta: &std::fs::Metadata, data: String) -> Result<Self, IntegrityWatcherError> {
        #[cfg(target_os = "linux")]
        let permissions = meta.permissions().mode();
        #[cfg(not(target_os = "linux"))]
        let permissions = meta.permissions().readonly() as u32;
        Ok(Self {
            data,
            permissions,
            modified: match meta.modified(){
                Ok(t) => t.duration_since(UNIX_EPOCH)?.as_secs(),
                Err(_) => 0,
            },
            size: meta.len().into(),
        })
    }
}

impl std::fmt::Display for SymlinkMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.modified as i64, 0){
            Some(t) =>
                write!(f, "-> {} perm: {:o} size: {} modified: {}", self.data, self.permissions, self.size, t),
            None => {
                write!(f, "-> {} perm: {:o} size: {} modified: #ERROR#", self.data, self.permissions, self.size)
            }
        }
    }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMetadata{
    pub hash: Hash,
    pub permissions: u32,
    pub modified: u64,
    pub size: ByteSize,
}

impl FileMetadata {
    pub fn new(meta: &std::fs::Metadata, hash: [u8; 32]) -> Result<Self, IntegrityWatcherError> {
        #[cfg(target_os = "linux")]
        let permissions = meta.permissions().mode();
        #[cfg(not(target_os = "linux"))]
        let permissions = meta.permissions().readonly() as u32;
        Ok(Self {
            hash: hash.into(),
            permissions,
            modified: match meta.modified(){
                Ok(t) => t.duration_since(UNIX_EPOCH)?.as_secs(),
                Err(_) => 0,
            },
            size: meta.len().into(),
        })
    }
}

impl std::fmt::Display for FileMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.modified as i64, 0){
            Some(t) =>
                write!(f, "hash: {} perm: {:o} size: {} modified: {}", self.hash, self.permissions, self.size, t),
            None => {
                write!(f, "hash: {} perm: {:o} size: {} modified: #ERROR#", self.hash, self.permissions, self.size)
            }
        }
    }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
pub struct DirMetadata{
    pub permissions: u32,
    pub modified: u64,
    pub size: u64,
}

impl DirMetadata {
    pub fn new(meta: &std::fs::Metadata) -> Result<Self, IntegrityWatcherError> {
        #[cfg(target_os = "linux")]
        let permissions = meta.permissions().mode();
        #[cfg(not(target_os = "linux"))]
        let permissions = meta.permissions().readonly() as u32;
        Ok(Self {
            permissions,
            modified: match meta.modified(){
                Ok(t) => t.duration_since(UNIX_EPOCH)?.as_secs(),
                Err(_) => 0,
            },
            size: meta.len(),
        })
    }
}

impl std::fmt::Display for DirMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.modified as i64, 0){
            Some(t) =>
                write!(f, " perm: {:o} size: {} modified: {}", self.permissions, self.size, t),
            None => {
                write!(f, " perm: {:o} size: {} modified: #ERROR#", self.permissions, self.size)
            }
        }
    }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
pub enum FileMetadataExt {
    Symlink(SymlinkMetadata),
    File(FileMetadata),
    Dir(DirMetadata),
}

impl std::fmt::Display for FileMetadataExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            FileMetadataExt::File(file) => {
                write!(f, "File {}", file)
            },
            FileMetadataExt::Symlink(symlink) => {
                write!(f, "Symlink {}", symlink)
            }
            FileMetadataExt::Dir(dir) => {
                write!(f, "Directory {}", dir)
            }
        }
    }
}

impl Value for FileMetadataExt {
    type SelfType<'a> = Self;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where Self: 'a{
        from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        to_allocvec(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileMetadata")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_sizes(){

        assert_eq!(format!("{}", ByteSize::new(0)), "0B");
        assert_eq!(format!("{}", ByteSize::new(1023)), "1023B");
        assert_eq!(format!("{}", ByteSize::new(1024)), "1.00KiB");
        assert_eq!(format!("{}", ByteSize::new(1024*1024)), "1.00MiB");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024)), "1.00GiB");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024)), "1.00TiB");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024*1024)), "1.00PiB");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024*1024*1024)), "1024.00PiB");
    }

    #[test]
    fn test_bandwidth(){

        let d = Duration::from_secs(1);
        assert_eq!(format!("{}", ByteSize::new(0).bandwidth(d)), "0.00B/s");
        assert_eq!(format!("{}", ByteSize::new(1024).bandwidth(d)), "1.00KiB/s");
        assert_eq!(format!("{}", ByteSize::new(1024*1024).bandwidth(d)), "1.00MiB/s");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024).bandwidth(d)), "1.00GiB/s");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024).bandwidth(d)), "1.00TiB/s");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024*1024).bandwidth(d)), "1.00PiB/s");
        assert_eq!(format!("{}", ByteSize::new(1024*1024*1024*1024*1024*1024).bandwidth(d)), "1024.00PiB/s");
    }
}