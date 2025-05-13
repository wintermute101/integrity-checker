use std::path::Path;
use std::collections::VecDeque;
use sha2::{Sha256, Digest};
use std::io;
use tokio::fs;
use tokio::task::JoinSet;
use redb::{Database, ReadableTable};
use log::{debug, error, warn, info, trace, LevelFilter};
use env_logger::Builder;
use clap::{Args, Parser};
use std::collections::HashSet;
use dirs::cache_dir;
use std::sync::Arc;

mod error;
mod types;
mod fileops;
mod cicrl;
use error::IntegrityWatcherError;
use types::{DirMetadata, FileMetadata, FileMetadataExt, SymlinkMetadata};
use fileops::{AddFileInfo, CheckDB, UpdateDB, WriteToDB, TABLE};

async fn get_file_hash(path: &Path) -> Result<FileMetadata, IntegrityWatcherError> {
    let mut hasher = Sha256::new();
    let mut file = std::fs::File::open(path)
        .map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
    io::copy(&mut file, &mut hasher)
        .map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
    let result = hasher.finalize();
    let meta = FileMetadata::new(&file.metadata().map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?, result.into())?;
    Ok(meta)
}

async fn visit_dirs<F>(dir: &Path, exclude: &HashSet<String>, finfo: &mut F) -> Result<(), IntegrityWatcherError>
    where F: AddFileInfo {
    type JoinReturn = Result<Option<(String, FileMetadataExt)>, IntegrityWatcherError>;
    let mut files: JoinSet<JoinReturn> = JoinSet::new();
    if exclude.contains(dir.to_str().unwrap()){
        warn!("Excluding top dir {}", dir.to_str().unwrap());
        return Ok(());
    }
    if dir.is_dir() && !dir.is_symlink() {
        let mut dqueue = VecDeque::new();
        dqueue.push_back(dir.to_owned());
        while let Some(dir) = dqueue.pop_front() {
            let mut direntry = match fs::read_dir(&dir).await
                .map_err(|e| IntegrityWatcherError::IOError { source: e, path: dir.to_string_lossy().to_string().to_owned() }){
                    Ok(e) => e,
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    }
                };

            while let Some(entry) = direntry.next_entry().await
                    .map_err(|e| IntegrityWatcherError::IOError { source: e, path: dir.to_string_lossy().to_string() })? {
                let path = entry.path();
                if exclude.contains(path.to_str().unwrap()){
                    debug!("Skipping {}", path.to_str().unwrap());
                    continue;
                }
                if path.is_dir() && !path.is_symlink() {
                    dqueue.push_back(path.to_owned());
                }
                let path_str = path.to_str().unwrap().to_owned();
                files.spawn(async move {
                    if path.is_file(){
                        let meta = get_file_hash(Path::new(&path)).await?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::File(meta))))
                    }
                    else if path.is_symlink() {
                        let data = fs::read_link(&path).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: path_str.to_owned() })?;
                        let meta = fs::symlink_metadata(&path).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_string_lossy().to_string() })?;
                        let sym = SymlinkMetadata::new(&meta, data.to_str().unwrap().to_owned())?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::Symlink(sym))))
                    }
                    else if path.is_dir(){
                        let meta = fs::metadata(path).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: path_str.to_owned() })?;
                        let dir = DirMetadata::new(&meta)?;
                        Ok(Some((path_str.to_owned(), FileMetadataExt::Dir(dir))))
                    }
                    else{
                        warn!("Path {} unsuported type", path.to_str().unwrap());
                        Ok(None)
                    }
                });

                let mut results = Vec::new();
                if files.len() > 128{ // writing to DB in bigger chunks is way faster
                    while let Some(result) = files.try_join_next() {
                        let result = result?;
                        match result{
                            Ok(Some(r)) => {
                                results.push(r);
                            }
                            Ok(None) => {},
                            Err(e) => {
                                error!("{e}");
                            }
                        }
                    }
                }
                if files.len() > 128{ //if we have too many files open we can crash need to throttle down
                    trace!("Too many files, waiting...");
                    let result = files.join_next().await.expect("we checked this in prev line")?;
                    match result{
                        Ok(Some(r)) => {
                            results.push(r);
                        }
                        Ok(None) => {},
                        Err(e) => {
                            error!("{e}");
                        }
                    }
                }
                if !results.is_empty(){
                    finfo.add_file_info(&results)?;
                }
            }
        }
    }
    else{
        let path = dir.to_str().unwrap().to_owned();
        let is_file = dir.is_file();
        let is_symlink = dir.is_symlink();
        files.spawn(async move {
            if is_file{
                let meta = get_file_hash(Path::new(&path)).await?;
                Ok(Some((path.to_owned(), FileMetadataExt::File(meta))))
            }
            else if is_symlink {
                let data = fs::read_link(&path).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_owned() })?;
                let meta = fs::symlink_metadata(&path).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: path.to_owned() })?;
                let sym = SymlinkMetadata::new(&meta, data.to_str().unwrap().to_owned())?;
                Ok(Some((path.to_owned(), FileMetadataExt::Symlink(sym))))
            }
            else{
                Ok(None)
            }
        });
    }

    let mut results = Vec::new();
    let res = files.join_all().await;
    for r in res{
        match r{
            Ok(Some(r)) => {
                results.push(r);
            }
            Ok(None) => {},
            Err(e) => {
                error!("{e}");
            }
        }
    };
    finfo.add_file_info(&results)?;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    cmd: Cmd,

    #[arg(long, default_value_t = String::from("files_data.redb"))]
    db: String,

    #[clap(group = "pathgroup", long, use_value_delimiter = true, value_delimiter = ',', num_args = 1.., help = "coma separated paths list")]
    path: Vec::<String>,

    #[clap(long, use_value_delimiter = true, value_delimiter = ',', num_args = 1.., help = "coma separated exlude paths list")]
    exclude: Vec::<String>,

    #[arg(long)]
    dont_exclude_db: bool,

    #[arg(long)]
    overwrite: bool,

    #[arg(long, help = "second DB for compare")]
    db2: Option<String>,

    #[arg(long, default_value_t = false)]
    compare_time: bool,

   #[arg(long, default_value_t = cache_dir().unwrap_or(std::path::PathBuf::from(".")).to_string_lossy().as_ref().to_owned() + "/cicrl_cache.redb")]

    cache: String,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
struct Cmd {
    #[arg(long, requires = "pathgroup", help = "creates DB and stores current files metadata")]
    create: bool,

    #[arg(long, requires = "pathgroup", help = "checks current files metadata compared to DB")]
    check: bool,

    #[arg(long, requires = "pathgroup", help = "updates DB")]
    update: bool,

    #[arg(long, help = "lists all files in DB")]
    list: bool,

    #[arg(long, help = "compares 2 databases (simmilar to check)")]
    compare: bool,

    #[arg(long, help = "check DB against CIRCL hashes https://www.circl.lu/services/hashlookup/")]
    circl_check: bool,
}

async fn main_fun() -> Result<(),IntegrityWatcherError> {
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
                    return Err(IntegrityWatcherError::IOError { source: e, path: args.db });
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
                    return Err(IntegrityWatcherError::IOError { source: e, path: args.db });
                }
            }
        }
        else if fs::try_exists(&args.db).await.map_err(|e| IntegrityWatcherError::IOError { source: e, path: args.db.to_owned() })?{
            error!("database {} already exists", &args.db);
            return Err(IntegrityWatcherError::IOError { source: io::Error::new(io::ErrorKind::AlreadyExists, "Already exists".to_owned()), path: args.db});
        }
        info!("Creating db {}", args.db);
        let db = Database::create(&args.db)?;
        let mut writer = WriteToDB::new(&db);
        for path in args.path.iter(){
            visit_dirs(Path::new(path), &exlude, &mut writer).await?;
        }
        info!("Added {} files", writer.get_counter());
    }

    if args.cmd.check{
        let db = Database::open(&args.db)?;
        let mut writer = CheckDB::new(&db, args.compare_time);

        for path in args.path.iter(){
            visit_dirs(Path::new(path), &exlude, &mut writer).await?;
        }

        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let iter = table.iter()?;

        for k in iter{
            let k = k?;
            if !writer.files.contains(&k.0.value()){
                warn!("File removed {} {}", k.0.value(), k.1.value())
            }
        }
        info!("Checked {} files", writer.files.len());
    }

    if args.cmd.update{
        let db = Database::open(&args.db)?;
        let mut writer = UpdateDB::new(&db);

        for path in args.path.iter(){
            visit_dirs(Path::new(path), &exlude, &mut writer).await?;
        }

        let mut to_remove = Vec::new();
            {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            let iter = table.iter()?;

            for k in iter{
                let k = k?;
                if !writer.files.contains(&k.0.value()){
                    to_remove.push(k.0.value());
                }
            }
        }
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for k in to_remove{
                info!("Removing file {}", k);
                table.remove(k)?;
            }
        }
        write_txn.commit()?;
        info!("Updated {} files", writer.get_counter());
    }

    if args.cmd.compare{
        let db2 = if let Some(dbname) = args.db2{
            Database::open(dbname)?
        }
        else{
            error!("Compare need db2 parameter");
            return Err(IntegrityWatcherError::IOError { source: io::Error::new(io::ErrorKind::InvalidData, "".to_owned()), path: "".to_owned()});
        };

        let db = Database::open(&args.db)?;

        let mut orig_files = Vec::new();

        let read_txn2 = db2.begin_read()?;
        let table2 = read_txn2.open_table(TABLE)?;
        let iter2 = table2.iter()?;

        for k in iter2{
            let k = k?;
            orig_files.push((k.0.value(), k.1.value()));
        }

        let mut writer = CheckDB::new(&db, args.compare_time);
        writer.add_file_info(&orig_files)?;

        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        let iter = table.iter()?;

        for k in iter{
            let k = k?;
            if !writer.files.contains(&k.0.value()){
                warn!("File removed {} {}", k.0.value(), k.1.value())
            }
        }
        info!("Checked {} files", writer.files.len());
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

    if args.cmd.circl_check{
        let db = Database::open(&args.db)?;
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        let iter = table.iter()?;

        let circl = Arc::new(cicrl::CirclQuery::new(&args.cache)?);
        type JoinReturn = Result<(String, types::Hash, Option<u8>), IntegrityWatcherError>;
        let mut queries: JoinSet<JoinReturn> = JoinSet::new();

        let fun = |q: JoinReturn| {
            match q{
                Ok((f, h, Some(v))) => {
                    info!("File {f} hash {h} found with score {v}");
                }
                Ok((f, h, None)) => {
                    warn!("File {f} hash {h} not found");
                }
                Err(e) => {
                    error!("Error query {e}");
                }
            }
        };
        for k in  iter{
            let k = k?;
            let meta = k.1.value();

            let fname = k.0.value().to_owned();
            if let FileMetadataExt::File(file_meta) = meta{
                let cc = circl.clone();
                queries.spawn( async move{
                    let h = file_meta.hash.clone();
                    let r = cc.query(&h).await?;
                    Ok((fname, h, r))
                });

                if queries.len() > 32{
                    loop {
                        if let Some(x) = queries.join_next().await{
                            let x = x?;
                            fun(x);
                        }
                        else{
                            break;
                        }
                        if queries.len() < 8{
                            break;
                        }
                    }
                }
            }
        }
        let r = queries.join_all().await;
        for i in r{
            fun(i);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(),IntegrityWatcherError> {
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