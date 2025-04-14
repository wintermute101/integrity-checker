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
    #[error("IO error {source} file {path}")]
    IOError{
        #[source]
        source: std::io::Error,
        path: String
    },

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
    modified: u64,
    size: u64,
}

impl SymlinkMetadata {
    pub fn new(meta: &std::fs::Metadata, data: String) -> Result<Self, ItegrityWatcherError> {
        Ok(Self {
            data,
            permissions: meta.permissions().mode(),
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
struct FileMetadata{
    hash: Hash,
    permissions: u32,
    modified: u64,
    size: u64,
}

impl FileMetadata {
    pub fn new(meta: &std::fs::Metadata, hash: [u8; 32]) -> Result<Self, ItegrityWatcherError> {
        Ok(Self {
            hash: hash.into(),
            permissions: meta.permissions().mode(),
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
struct DirMetadata{
    permissions: u32,
    modified: u64,
    size: u64,
}

impl DirMetadata {
    pub fn new(meta: &std::fs::Metadata) -> Result<Self, ItegrityWatcherError> {
        Ok(Self {
            permissions: meta.permissions().mode(),
            modified: meta.created().map_err(|e| ItegrityWatcherError::IOError { source: e, path: "unknown".to_owned() })?.duration_since(UNIX_EPOCH)?.as_secs(),
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
enum FileMetadataExt {
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

const TABLE: TableDefinition<String, FileMetadataExt> = TableDefinition::new("files_database");

async fn get_file_hash(path: &Path) -> Result<FileMetadata, ItegrityWatcherError> {
    let mut hasher = Sha256::new();
    let mut file = std::fs::File::open(path)
        .map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
    io::copy(&mut file, &mut hasher)
        .map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
    let result = hasher.finalize();
    let meta = FileMetadata::new(&file.metadata().map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?, result.into())?;
    Ok(meta)
}

async fn visit_dirs<F>(dir: &Path, exclude: &HashSet<String>, fun: &mut F) -> Result<(), ItegrityWatcherError>
    where F: FnMut(&Vec<(String, FileMetadataExt)>) -> Result<(), ItegrityWatcherError> {
    type JoinReturn = Result<Option<(String, FileMetadataExt)>, ItegrityWatcherError>;
    let mut files: Vec<tokio::task::JoinHandle<JoinReturn>> = vec![];
    if exclude.contains(dir.to_str().unwrap()){
        warn!("Excluding top dir {}", dir.to_str().unwrap());
        return Ok(());
    }
    if dir.is_dir() && !dir.is_symlink() {
        let mut dqueue = VecDeque::new();
        dqueue.push_back(dir.to_owned());
        while let Some(dir) = dqueue.pop_front() {
            let mut direntry = fs::read_dir(&dir).await
                .map_err(|e| ItegrityWatcherError::IOError { source: e, path: dir.to_string_lossy().to_string().to_owned() })?;

            while let Some(entry) = direntry.next_entry().await
                    .map_err(|e| ItegrityWatcherError::IOError { source: e, path: dir.to_string_lossy().to_string() })? {
                let path = entry.path();
                if exclude.contains(path.to_str().unwrap()){
                    debug!("Skipping {}", path.to_str().unwrap());
                    continue;
                }
                if path.is_dir() && !path.is_symlink() {
                    dqueue.push_back(path.to_owned());
                }
                let path_str = path.to_str().unwrap().to_owned();
                let s: task::JoinHandle<JoinReturn> = tokio::spawn(async move {
                    if path.is_file(){
                        let meta = get_file_hash(Path::new(&path)).await?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::File(meta))))
                    }
                    else if path.is_symlink() {
                        let data = fs::read_link(&path).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: path_str.to_owned() })?;
                        let meta = fs::symlink_metadata(&path).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
                        let sym = SymlinkMetadata::new(&meta, data.to_str().unwrap().to_owned())?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::Symlink(sym))))
                    }
                    else if path.is_dir(){
                        let meta = fs::metadata(path).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: path_str.to_owned() })?;
                        let dir = DirMetadata::new(&meta)?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::Dir(dir))))
                    }
                    else{
                        warn!("Path {} unsuported type", path.to_str().unwrap());
                        Ok(None)
                    }
                });
                if files.len() >= 32{
                    let mut table = Vec::new();
                    let mut new_files = Vec::new();
                    for i in files{
                        if i.is_finished()
                        {
                            let ret = match i.await?{
                                Ok(r) => r,
                                Err(e) => {
                                    error!("Error {}",e);
                                    None
                                }
                            };
                            if let Some(r) = ret{
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
                    let ret = match s.await?{
                        Ok(r) => r,
                        Err(e) => {
                            error!("Error {}",e);
                            None
                        }
                    };
                    if let Some(r) = ret{
                        fun(&vec![r])?;
                    }
                }
                else{
                    files.push(s);
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
                let data = fs::read_link(&path).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_owned() })?;
                let meta = fs::symlink_metadata(&path).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: path.to_owned() })?;
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
        let ret = match i.await?{
            Ok(r) => r,
            Err(e) => {
                error!("Error {}",e);
                None
            }
        };
        if let Some(r) = ret{
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
                    (FileMetadataExt::Dir(f), FileMetadataExt::Symlink(s)) => {
                        error!("{} Dir {} changed to symlink {}", k, f, s);
                    },
                    (FileMetadataExt::Dir(f), FileMetadataExt::File(s)) => {
                        error!("{} Dir {} changed to file {}", k, f, s);
                    },
                    (FileMetadataExt::Symlink(f), FileMetadataExt::Dir(s)) => {
                        error!("{} Symlink {} changed to dir {}", k, f, s);
                    },
                    (FileMetadataExt::File(f), FileMetadataExt::Dir(s)) => {
                        error!("{} File {} changed to dir {}", k, f, s);
                    },
                    (FileMetadataExt::Dir(old), FileMetadataExt::Dir(new)) => {
                        if old.modified != new.modified{
                            let t1: String = match DateTime::from_timestamp(old.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            let t2: String = match DateTime::from_timestamp(new.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            info += &format!(" modified time changed {} -> {}", t1, t2);
                        }
                        if old.permissions != new.permissions{
                            info += &format!(" permissions changed {:o} -> {:o}", old.permissions, new.permissions);
                        }
                        if old.size != new.size{
                            info += &format!(" size changed {} -> {}", old.size, new.size);
                        }
                        error!("Dir {} changed:{}", k, info);
                    },
                    (FileMetadataExt::File(old), FileMetadataExt::File(new)) => {
                        if old.hash != new.hash{
                            info = format!(" hash changed {} -> {}", old.hash, new.hash);
                        }
                        if old.modified != new.modified{
                            let t1: String = match DateTime::from_timestamp(old.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            let t2: String = match DateTime::from_timestamp(new.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            info += &format!(" modified time changed {} -> {}", t1, t2);
                        }
                        if old.permissions != new.permissions{
                            info += &format!(" permissions changed {:o} -> {:o}", old.permissions, new.permissions);
                        }
                        if old.size != new.size{
                            info += &format!(" size changed {} -> {}", old.size, new.size);
                        }
                        error!("File {} changed:{}", k, info);
                    },
                    (FileMetadataExt::Symlink(old), FileMetadataExt::Symlink(new)) => {

                        if old.data != new.data{
                            info = format!(" changed {} -> {}", old.data, new.data);
                        }
                        if old.modified != new.modified{
                            let t1: String = match DateTime::from_timestamp(old.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            let t2: String = match DateTime::from_timestamp(new.modified as i64, 0){
                                Some(t) => t.to_string(),
                                None => "#ERROR#".to_owned(),
                            };
                            info += &format!(" modified time changed {} -> {}", t1, t2);
                        }
                        if old.permissions != new.permissions{
                            info += &format!(" permissions changed {:o} -> {:o}", old.permissions, new.permissions);
                        }
                        if old.size != new.size{
                            info += &format!(" size changed {} -> {}", old.size, new.size);
                        }
                        error!("Symlink {} changed:{}", k, info);
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

    #[clap(long, use_value_delimiter = true, value_delimiter = ',', num_args = 1..)]
    exclude: Vec::<String>,

    #[arg(long)]
    dont_exclude_db: bool,

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

async fn main_fun() -> Result<(),ItegrityWatcherError> {
    let mut args = Cli::parse();
    Builder::new()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .format_timestamp(None)
        .target(env_logger::Target::Stdout)
        .init();

    if !args.dont_exclude_db{
        let db_path = std::path::PathBuf::from(&args.db);
        match fs::canonicalize(db_path).await{
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound{
                    return Err(ItegrityWatcherError::IOError { source: e, path: args.db });
                }
            }
            Ok(f) => {
                args.exclude.push(f.to_str().unwrap().to_owned());
            }
        };
        args.exclude.push(args.db.to_owned());
    }
    debug!("Paths {:?}", args.path);
    debug!("Excluded {:?}", args.exclude);

    let mut exlude = HashSet::new();

    for i in args.exclude{
        exlude.insert(i);
    }

    if args.cmd.create{
        if args.overwrite{
            if let Err(e) = fs::remove_file(&args.db).await{
                if e.kind() != std::io::ErrorKind::NotFound{
                    return Err(ItegrityWatcherError::IOError { source: e, path: args.db });
                }
            }
        }
        else if fs::try_exists(&args.db).await.map_err(|e| ItegrityWatcherError::IOError { source: e, path: args.db.to_owned() })?{
            error!("database {} already exists", &args.db);
            return Err(ItegrityWatcherError::IOError { source: io::Error::new(io::ErrorKind::AlreadyExists, "Already exists".to_owned()), path: args.db});
        }
        info!("Creating db {}", args.db);
        let db = Database::create(&args.db)?;
        let mut cnt = 0;
        for path in args.path.iter(){
            visit_dirs(Path::new(path), &exlude,&mut|data| write_to_db(&db, data, &mut cnt)).await?;
        }
        info!("Added {} files", cnt);
    }

    if args.cmd.check{
        let db = Database::open(&args.db)?;
        let mut files = HashSet::new();
        for path in args.path.iter(){
            visit_dirs(Path::new(path), &exlude, &mut |data| check_db(&db, data, &mut files)).await?;
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
            visit_dirs(Path::new(path), &exlude, &mut|data| update_db(&db, data, &mut cnt)).await?;
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

#[tokio::main]
async fn main() -> Result<(),ItegrityWatcherError> {
    match main_fun().await{
        Err(e) => {
            error!("Error {}", e);
            Err(e)
        },
        Ok(()) => {
            Ok(())
        }
    }
}