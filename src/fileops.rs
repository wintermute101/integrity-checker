use super::types::FileMetadataExt;
use super::error::IntegrityWatcherError;
use log::{debug, error, warn, info, trace};
use redb::{Database, TableDefinition};
use std::collections::HashSet;
use chrono::DateTime;

pub const TABLE: TableDefinition<String, FileMetadataExt> = TableDefinition::new("files_database");

pub trait AddFileInfo {
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError>;
}

pub struct WriteToDB<'ldb>{
    counter: u64,
    db: &'ldb Database,
}

impl<'ldb> WriteToDB<'ldb>{
    pub fn new(db: &'ldb Database) -> Self{
        WriteToDB{ db, counter: 0}
    }

    pub fn get_counter(&self) -> u64{
        self.counter
    }
}

impl AddFileInfo for WriteToDB<'_>{
    fn add_file_info(&mut self, data: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for (k,v) in data{
                trace!("Adding file {}", k);
                table.insert(k, v)?;
                self.counter+=1;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}

pub struct UpdateDB<'ldb>{
    db: &'ldb Database,
    counter: u64,
    pub files: HashSet<String>
}

impl<'ldb> UpdateDB<'ldb> {
    pub fn new(db: &'ldb Database) -> Self{
        UpdateDB{ db, counter: 0, files: HashSet::new() }
    }

    pub fn get_counter(&self) -> u64{
        self.counter
    }
}

impl AddFileInfo for UpdateDB<'_>{
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for (k,v) in files{
                self.counter+=1;
                self.files.insert(k.to_owned());
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
}

pub struct CheckDB<'ldb>{
    db: &'ldb Database,
    pub files: HashSet<String>,
    compare_time: bool,
}

impl<'ldb> CheckDB<'ldb>{
    pub fn new(db: &'ldb Database, compare_time: bool) -> Self{
        CheckDB { db, files: HashSet::new(), compare_time }
    }
}

impl AddFileInfo for CheckDB<'_> {
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        for (k, v) in files{
            self.files.insert(k.to_owned());

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
                            let mut only_time_modified = true;
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
                                only_time_modified = false;
                            }
                            if old.size != new.size{
                                info += &format!(" size changed {} -> {}", old.size, new.size);
                                only_time_modified = false;
                            }
                            if !only_time_modified || self.compare_time{
                                error!("Dir {} changed:{}", k, info);
                            }
                        },
                        (FileMetadataExt::File(old), FileMetadataExt::File(new)) => {
                            let mut only_time_modified = true;
                            if old.hash != new.hash{
                                info = format!(" hash changed {} -> {}", old.hash, new.hash);
                                only_time_modified = false;
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
                                only_time_modified = false;
                            }
                            if old.size != new.size{
                                info += &format!(" size changed {} -> {}", old.size, new.size);
                                only_time_modified = false;
                            }
                            if !only_time_modified || self.compare_time{
                                error!("File {} changed:{}", k, info);
                            }
                        },
                        (FileMetadataExt::Symlink(old), FileMetadataExt::Symlink(new)) => {
                            let mut only_time_modified = true;
                            if old.data != new.data{
                                info = format!(" changed {} -> {}", old.data, new.data);
                                only_time_modified = false;
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
                                only_time_modified = false;
                            }
                            if old.size != new.size{
                                info += &format!(" size changed {} -> {}", old.size, new.size);
                                only_time_modified = false;
                            }
                            if !only_time_modified || self.compare_time{
                                error!("Symlink {} changed:{}", k, info);
                            }
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
}
