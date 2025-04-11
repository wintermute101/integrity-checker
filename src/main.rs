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
use std::time::UNIX_EPOCH;
use chrono::DateTime;

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
struct SymlinkMetadata{
    data: String,
    permissions: u32,
    created: u64,
    size: u64,
}

impl SymlinkMetadata {
    pub fn new(meta: &std::fs::Metadata, data: String) -> Result<Self, ItegrityWatcherError> {
        Ok(Self {
            data,
            permissions: meta.permissions().mode(),
            created: match meta.modified(){
                Ok(t) => t.duration_since(UNIX_EPOCH)?.as_secs(),
                Err(_) => 0,
            },
            size: meta.len(),
        })
    }
}

impl std::fmt::Display for SymlinkMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.created as i64, 0){
            Some(t) =>
                write!(f, "-> {} perm: {:o} size: {} modified: {}", self.data, self.permissions, self.size, t),
            None => {
                write!(f, "-> {} perm: {:o} size: {} modified: #ERROR#", self.data, self.permissions, self.size)
            }
        }
    }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
struct FileMetadata{
    hash: Hash,
    permissions: u32,
    created: u64,
    size: u64,
}

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

impl std::fmt::Display for FileMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match DateTime::from_timestamp(self.created as i64, 0){
            Some(t) =>
                write!(f, "hash: {} perm: {:o} size: {} modified: {}", self.hash, self.permissions, self.size, t),
            None => {
                write!(f, "hash: {} perm: {:o} size: {} modified: #ERROR#", self.hash, self.permissions, self.size)
            }
        }
    }
}

#[derive(Debug,Serialize, Deserialize, PartialEq, Eq)]
enum FileMetadataExt {
    Symlink(SymlinkMetadata),
    File(FileMetadata),
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

const TABLE: TableDefinition<String, FileMetadataExt> = TableDefinition::new("files_database");

async fn get_file_hash(path: &Path) -> Result<FileMetadata, ItegrityWatcherError> {
    let mut hasher = Sha256::new();
    let mut file = std::fs::File::open(path)?;
    io::copy(&mut file, &mut hasher)?;
    let result = hasher.finalize();
    let meta = FileMetadata::new(&file.metadata()?, result.into())?;
    Ok(meta)
}

async fn visit_dirs<F>(dir: &Path, fun: &mut F) -> Result<(), ItegrityWatcherError>
    where F: FnMut(&Vec<(String, FileMetadataExt)>) -> Result<(), ItegrityWatcherError> {
    type JoinReturn = Result<Option<(String, FileMetadataExt)>, ItegrityWatcherError>;
    let mut files: Vec<tokio::task::JoinHandle<JoinReturn>> = vec![];
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
                    let s: task::JoinHandle<JoinReturn> = tokio::spawn(async move {
                        if path.is_file(){
                            let path = path.to_str().unwrap();
                            let meta = get_file_hash(Path::new(&path)).await?;
                            Ok(Some((path.to_owned(), FileMetadataExt::File(meta))))
                        }
                        else if path.is_symlink() {
                            let path = path.to_str().unwrap();
                            let data = fs::read_link(path).await?;
                            let meta = fs::symlink_metadata(path).await?;
                            let sym = SymlinkMetadata::new(&meta, data.to_str().unwrap().to_owned())?;
                            Ok(Some((path.to_owned(), FileMetadataExt::Symlink(sym))))
                        }
                        else{
                            warn!("Path is not file or symlink {} unsuported", path.to_str().unwrap());
                            Ok(None)
                        }
                    });
                    if files.len() >= 32{
                        let mut table = Vec::new();
                        let mut new_files = Vec::new();
                        for i in files{
                            if i.is_finished()
                            {
                                if let Some(r) = i.await??{
                                    table.push(r);
                                }
                            }
                            else {
                                new_files.push(i);
                            }
                        }
                        fun(&table)?;
                        files = new_files;
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
        let is_file = dir.is_file();
        let is_symlink = dir.is_symlink();
        let s: tokio::task::JoinHandle<JoinReturn> = tokio::spawn(async move {
            if is_file{
                let meta = get_file_hash(Path::new(&path)).await?;
                Ok(Some((path.to_owned(), FileMetadataExt::File(meta))))
            }
            else if is_symlink {
                let data = fs::read_link(&path).await?;
                let meta = fs::symlink_metadata(&path).await?;
                let sym = SymlinkMetadata::new(&meta, data.to_str().unwrap().to_owned())?;
                Ok(Some((path.to_owned(), FileMetadataExt::Symlink(sym))))
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

fn write_to_db(db: &Database, data: &Vec<(String, FileMetadataExt)>, cnt: &mut u32) -> Result<(), ItegrityWatcherError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        for (k,v) in data{
            trace!("Adding file {}", k);
            table.insert(k, v)?;
            *cnt+=1;
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn update_db(db: &Database, data: &Vec<(String, FileMetadataExt)>, cnt: &mut u32) -> Result<(), ItegrityWatcherError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        for (k,v) in data{
            *cnt+=1;
            if let Some(old) = table.insert(k, v)?{
                if old.value() != *v{
                    info!("File updated {} {} -> {}", k, old.value(), v);
                }
            }
            else{
                info!("New file {} {}", k, v);
            }
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn check_db(db: &Database, data: &Vec<(String, FileMetadataExt)>, files: &mut HashSet<String>) -> Result<(), ItegrityWatcherError> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    for (k, v) in data{
        files.insert(k.to_owned());

        if let Some(oldv) = table.get(k)?{
            if oldv.value() != *v{
                let old_val = oldv.value();
                let mut info = String::new();

                match (old_val, v)
                {
                    (FileMetadataExt::Symlink(s), FileMetadataExt::File(f)) => {
                        error!("{} Symlink {} changed to file {}", k, s, f);
                    },
                    (FileMetadataExt::File(f), FileMetadataExt::Symlink(s)) => {
                        error!("{} File {} changed to symlink {}", k, f, s);
                    },
                    (FileMetadataExt::File(old), FileMetadataExt::File(new)) => {
                        if old.hash != new.hash{
                            info = format!(" hash changed {} -> {}", old.hash, new.hash);
                        }
                        if old.created != new.created{
                            let t1: String = match DateTime::from_timestamp(old.created as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            let t2: String = match DateTime::from_timestamp(new.created as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            info += &format!(" created time changed {} -> {}", t1, t2);
                        }
                        if old.permissions != new.permissions{
                            info += &format!(" permissions changed {:o} -> {:o}", old.permissions, new.permissions);
                        }
                        if old.size != new.size{
                            info += &format!(" size changed {} -> {}", old.size, new.size);
                        }
                        error!("File {} changed {}", k, info);
                    },
                    (FileMetadataExt::Symlink(old), FileMetadataExt::Symlink(new)) => {

                        if old.data != new.data{
                            info = format!(" changed {} -> {}", old.data, new.data);
                        }
                        if old.created != new.created{
                            let t1: String = match DateTime::from_timestamp(old.created as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            let t2: String = match DateTime::from_timestamp(new.created as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            info += &format!(" created time changed {} -> {}", t1, t2);
                        }
                        if old.permissions != new.permissions{
                            info += &format!(" permissions changed {:o} -> {:o}", old.permissions, new.permissions);
                        }
                        if old.size != new.size{
                            info += &format!(" size changed {} -> {}", old.size, new.size);
                        }
                        error!("Symlink {} changed {}", k, info);
                    }
                }
            }
            else {
                debug!("File ok {}", k);
            }
        }
        else{
            warn!("New file {} {}", k, v);
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

    #[clap(group = "pathgroup", long, use_value_delimiter = true, value_delimiter = ',', num_args = 1..)]
    path: Vec::<String>,

    #[arg(long)]
    overwrite: bool,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
struct Cmd {
    #[arg(long, requires = "pathgroup")]
    create: bool,

    #[arg(long, requires = "pathgroup")]
    check: bool,

    #[arg(long, requires = "pathgroup")]
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


    info!("args path {:?}", args.path);

    if args.cmd.create{
        if args.overwrite{
            fs::remove_file(&args.db).await?;
        }
        else if fs::try_exists(&args.db).await?{
            error!("database {} already exists", &args.db);
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, args.db).into());
        }
        let db = Database::create(&args.db)?;
        let mut cnt = 0;
        for path in args.path.iter(){
            visit_dirs(Path::new(path),&mut|data| write_to_db(&db, data, &mut cnt)).await?;
        }
        info!("Added {} files", cnt);
    }

    if args.cmd.check{
        let db = Database::open(&args.db)?;
        let mut files = HashSet::new();
        for path in args.path.iter(){
            visit_dirs(Path::new(path), &mut |data| check_db(&db, data, &mut files)).await?;
        }

        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let iter = table.iter()?;

        for k in iter{
            let k = k?;
            if !files.contains(&k.0.value()){
                warn!("File removed {} {}", k.0.value(), k.1.value())
            }
        }
        info!("Checked {} files", files.len());
    }

    if args.cmd.update{
        let db = Database::open(&args.db)?;
        let mut cnt = 0;
        for path in args.path.iter(){
            visit_dirs(Path::new(path), &mut|data| update_db(&db, data, &mut cnt)).await?;
        }
        info!("Checked {} files", cnt);
    }

    if args.cmd.list{
        let db = Database::open(&args.db)?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        let iter = table.iter()?;

        for k in  iter{
            let k = k?;
            info!("File: {}: {}", k.0.value(), k.1.value());
        }
    }

    Ok(())
}
