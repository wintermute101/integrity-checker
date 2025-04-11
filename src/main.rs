use thiserror::Error;
use std::path::Path;
use std::collections::VecDeque;
use sha2::{Sha256, Digest};
use std::io;
use tokio::fs;
use tokio::task;
use redb::{Database, ReadableTable, TableDefinition, Value};
use serde::{Serialize, Deserialize};
use std::os::unix::fs::PermissionsExt;
use postcard::{from_bytes, to_allocvec};
use log::{debug, error, warn, info, trace, LevelFilter};
use env_logger::Builder;
use clap::{Parser, Args};
use std::collections::HashSet;
use std::cell::Cell;
use std::time::UNIX_EPOCH;
use chrono::{DateTime, Utc};

#[derive(Error, Debug)]
pub enum ItegrityWatcherError {
    #[error("IO error {0}")]
    IOError(#[from] std::io::Error),

    #[error("Join error {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("DB error {0}")]
    DBError(#[from] redb::DatabaseError),

    #[error("DB Storage error {0}")]
    DBStorageError(#[from] redb::StorageError),

    #[error("DB Transaction error {0}")]
    DBTransactionError(#[from] redb::TransactionError),

    #[error("DB Table error {0}")]
    DBTableError(#[from] redb::TableError),

    #[error("DB Commit error {0}")]
    DBCommitError(#[from] redb::CommitError),

    #[error("SystemTimeError error {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError)

}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Hash{
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

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
struct FileMetadata{
    hash: Hash,
    permissions: u32,
    created: u64,
    size: u64,
}

impl Value for FileMetadata {
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

impl std::fmt::Display for FileMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.created as i64, 0){
            Some(t) =>
                write!(f, "hash: {} perm: {:o} size: {} created: {}", self.hash, self.permissions, self.size, t),
            None => {
                write!(f, "hash: {} perm: {:o} size: {} created: #ERROR#", self.hash, self.permissions, self.size)
            }
        }
    }
}

const TABLE: TableDefinition<String, FileMetadata> = TableDefinition::new("files_database");

impl FileMetadata {
    pub fn new(meta: &std::fs::Metadata, hash: [u8; 32]) -> Result<Self, ItegrityWatcherError> {
        Ok(Self {
            hash: hash.into(),
            permissions: meta.permissions().mode(),
            created: meta.created()?.duration_since(UNIX_EPOCH)?.as_secs(),
            size: meta.len(),
        })
    }
}

async fn get_file_hash(path: &Path) -> Result<Option<FileMetadata>, ItegrityWatcherError> {
    let mut hasher = Sha256::new();
    let mut file = match std::fs::File::open(path){
        Ok(f) => f,
        Err(e) =>{
            if e.kind() == std::io::ErrorKind::NotFound{
                return Ok(None);
            }
            else{
                return Err(e.into());
            }
        }
    };
    io::copy(&mut file, &mut hasher)?;
    let result = hasher.finalize();
    let meta = FileMetadata::new(&file.metadata()?, result.into())?;
    Ok(Some(meta))
}

async fn visit_dirs<F>(dir: &Path, fun: &mut F) -> Result<(), ItegrityWatcherError>
    where F: FnMut(&Vec<(String, FileMetadata)>) -> Result<(), ItegrityWatcherError> {
    let mut files: Vec<tokio::task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>>> = vec![];
    if dir.is_dir() {
        let mut dqueue = VecDeque::new();
        dqueue.push_back(dir.to_owned());
        while let Some(dir) = dqueue.pop_front() {
            let mut dir = fs::read_dir(dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    dqueue.push_back(path);
                } else {
                    if path.is_symlink(){
                        //println!("Symlink: {:?}", path);
                        let meta = fs::symlink_metadata(&path).await?;
                        let sympath = fs::read_link(path).await?;

                        println!("Symlink: {} {} len {}", sympath.as_path().to_str().unwrap(), meta.permissions().mode(), meta.len());

                        continue;
                    }
                    let s: task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>> = tokio::spawn(async move {
                        let path = path.to_str().unwrap();
                        if let Some(meta) = get_file_hash(Path::new(&path)).await?{
                            //println!("File: {:?}, Hash: {:x?}", path, meta);
                            Ok(Some((path.to_owned(), meta)))
                        }
                        else{
                            error!("get file hash none {}", path);
                            Ok(None)
                        }
                    });
                    if files.len() >= 32{
                        let mut table = Vec::new();
                        for i in files.iter_mut(){
                            if i.is_finished()
                            {
                                if let Some(r) = i.await??{
                                    table.push(r);
                                }
                            }
                        }
                        fun(&table)?;
                        files = files.into_iter().filter(|f| !f.is_finished()).collect();
                    }
                    if files.len() >= 64 {
                        trace!("Too many files, waiting...");
                        if let Some(r) = s.await??{
                            fun(&vec![r])?;
                        }
                    }
                    else{
                        files.push(s);
                    }
                }
            }
        }
    }
    else{
        let path = dir.to_str().unwrap().to_owned();
        let s: tokio::task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>> = tokio::spawn(async move {
            if let Some(meta) = get_file_hash(Path::new(&path)).await?{
                //println!("File: {:?}, Hash: {:?}", path, meta);
                Ok(Some((path.to_owned(), meta)))
            }
            else{
                Ok(None)
            }
        });
        files.push(s);
    }

    let mut table = Vec::new();
    for i in files.iter_mut(){
        if let Some(r) = i.await??{
            table.push(r);
        }
    }
    fun(&table)?;

    Ok(())
}

fn write_to_db(db: &Database, data: &Vec<(String, FileMetadata)>) -> Result<(), ItegrityWatcherError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        for (k,v) in data{
            trace!("Adding file {}", k);
            table.insert(k, v)?;
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn update_db(db: &Database, data: &Vec<(String, FileMetadata)>) -> Result<(), ItegrityWatcherError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        for (k,v) in data{
            if let Some(old) = table.insert(k, v)?{
                if old.value() != *v{
                    info!("File updated {} {} -> {}", k, old.value().hash, v.hash);
                }
            }
            else{
                info!("New file {} {}", k, v.hash);
            }
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn check_db(db: &Database, data: &Vec<(String, FileMetadata)>, files: &mut HashSet<String>) -> Result<(), ItegrityWatcherError> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    for (k, v) in data{
        files.insert(k.to_owned());

        if let Some(sv) = table.get(k)?{
            if sv.value() != *v{
                let val = sv.value();
                error!("File {} changed", k);
                if val.hash != v.hash{
                    error!("File hash changed {} -> {}", val.hash, v.hash);
                }
                if val.created != v.created{
                    error!("File created time changed");
                }
                if val.permissions != v.permissions{
                    error!("File permissions changed {:o} -> {:o}", val.permissions, v.permissions);
                }
                if val.size != v.size{
                    error!("File size changed {} -> {}", val.size, v.size);
                }
            }
            else {
                debug!("File ok {}", k);
            }
        }
        else{
            info!("New file {} {:x?}", k, v.hash);
        }
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    cmd: Cmd,

    #[arg(long, default_value_t = String::from("files_data.redb"))]
    db: String,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
struct Cmd {
    #[arg(long)]
    create: bool,

    #[arg(long)]
    check: bool,

    #[arg(long)]
    update: bool,

    #[arg(long)]
    list: bool,
}

#[tokio::main]
async fn main() -> Result<(),ItegrityWatcherError> {
    let args = Cli::parse();
    Builder::new()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .format_timestamp(None)
        .target(env_logger::Target::Stdout)
        .init();

    if args.cmd.create{
        if fs::try_exists(&args.db).await?{
            error!("database {} already exists", &args.db);
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, args.db).into());
        }
        let db = Database::create(&args.db)?;
        visit_dirs(&Path::new("tests/"),&mut|data| write_to_db(&db, data)).await?;
    }

    if args.cmd.check{
        let db = Database::open(&args.db)?;
        let mut files = HashSet::new();
        visit_dirs(&Path::new("tests/"), &mut |data| check_db(&db, data, &mut files)).await?;

        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let mut iter = table.iter()?;

        while let Some(k) =  iter.next(){
            let k = k?;
            if !files.contains(&k.0.value()){
                warn!("File removed {} {}", k.0.value(), k.1.value())
            }
        }
        info!("Checked {} files", files.len());
    }

    if args.cmd.update{
        let db = Database::open(&args.db)?;
        visit_dirs(&Path::new("tests/"), &mut|data| update_db(&db, data)).await?;
    }

    if args.cmd.list{
        let db = Database::open(&args.db)?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        let mut iter = table.iter()?;

        while let Some(k) =  iter.next(){
            let k = k?;
            info!("File: {}: {}", k.0.value(), k.1.value());
        }
    }

    Ok(())
}
