use thiserror::Error;
use std::path::Path;
use std::collections::VecDeque;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::io;
use tokio::fs;
use tokio::task;

#[derive(Error, Debug)]
pub enum ItegrityWatcherError {
    #[error("IO error {0}")]
    IOError(#[from] std::io::Error),

    #[error("Join error {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
struct FileMetadata{
    hash: [u8;32],
    permissions: std::fs::Permissions,
    created: std::time::SystemTime,
    size: u64,
}

impl FileMetadata {
    pub fn new(meta: &std::fs::Metadata, hash: [u8; 32]) -> io::Result<Self> {
        Ok(Self {
            hash,
            permissions: meta.permissions(),
            created: meta.created()?,
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

async fn visit_dirs(dir: &Path) -> Result<(), ItegrityWatcherError> {
    let mut files: Vec<_> = vec![];
    let mut results = HashMap::new();
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
                        println!("Symlink: {:?}", path);
                        continue;
                    }
                    let s: task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>> = tokio::spawn(async move {
                        let path = path.to_str().unwrap();
                        if let Some(meta) = get_file_hash(Path::new(&path)).await?{
                            println!("File: {:?}, Hash: {:x?}", path, meta);
                            Ok(Some((path.to_owned(), meta)))
                        }
                        else{
                            Ok(None)
                        }
                    });
                    if files.len() >= 32{
                        for i in files.iter_mut(){
                            let ii: &mut task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>> = i;
                            if ii.is_finished()
                            {
                                if let Some((f,h)) = i.await??{
                                    results.insert(f, h);
                                }
                            }
                        }
                        files = files.into_iter().filter(|f| !f.is_finished()).collect();
                    }
                    else if files.len() >= 64 {
                        if let Some((f,h)) = s.await??{
                            results.insert(f, h);
                        }
                    }
                    else{
                        files.push(s);
                    }
                    println!("Num of files {}", files.len());
                }
            }
        }
    }
    else{
        let path = dir.to_str().unwrap().to_owned();
        let s: tokio::task::JoinHandle<Result<Option<(String, FileMetadata)>, ItegrityWatcherError>> = tokio::spawn(async move {
            if let Some(meta) = get_file_hash(Path::new(&path)).await?{
                println!("File: {:?}, Hash: {:?}", path, meta);
                Ok(Some((path.to_owned(), meta)))
            }
            else{
                Ok(None)
            }
        });
        files.push(s);
    }

    for res in files{
        if let Some(f) = res.await??{
            results.insert(f.0, f.1);
        }
    }

     println!("Files {}", results.len());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(),ItegrityWatcherError> {

    visit_dirs(&Path::new("/etc")).await?;

    Ok(())
}
