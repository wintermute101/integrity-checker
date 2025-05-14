use serde::{Serialize, Deserialize};
use postcard::{from_bytes, to_allocvec};
use std::time::UNIX_EPOCH;
use chrono::DateTime;
use redb::{Value,Key};

#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;

use super::error::IntegrityWatcherError;

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
    pub size: u64,
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
            size: meta.len(),
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
    pub size: u64,
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
            size: meta.len(),
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
