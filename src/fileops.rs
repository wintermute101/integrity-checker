use super::types::{FileMetadataExt, ByteSize};
use super::error::IntegrityWatcherError;
use log::{debug, error, warn, info, trace};
use redb::{Database, TableDefinition, ReadableDatabase};
use std::collections::HashSet;
use chrono::DateTime;

pub const TABLE: TableDefinition<String, FileMetadataExt> = TableDefinition::new("files_database");

pub trait AddFileInfo {
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError>;
}

pub struct WriteToDB<'ldb>{
    counter: u64,
    byte_counter: ByteSize,
    db: &'ldb Database,
}

impl<'ldb> WriteToDB<'ldb>{
    pub fn new(db: &'ldb Database) -> Self{
        WriteToDB{ db, counter: 0, byte_counter: ByteSize::default()}
    }

    pub fn get_counter(&self) -> u64{
        self.counter
    }

    pub fn get_bytes(&self) -> ByteSize{
        self.byte_counter
    }
}

impl AddFileInfo for WriteToDB<'_>{
    fn add_file_info(&mut self, data: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {
        let write_txn = self.db.begin_write().map_err(Box::new)?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for (k,v) in data{
                trace!("Adding file {}", k);
                match v{
                    FileMetadataExt::Dir(_) => {},
                    FileMetadataExt::File(file) => self.byte_counter.add_size(&file.size),
                    FileMetadataExt::Symlink(symlink) => self.byte_counter.add_size(&symlink.size),
                }
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
    byte_counter: ByteSize,
    pub files: HashSet<String>
}

impl<'ldb> UpdateDB<'ldb> {
    pub fn new(db: &'ldb Database) -> Self{
        UpdateDB{ db, counter: 0, byte_counter: ByteSize::default(), files: HashSet::new() }
    }

    pub fn get_counter(&self) -> u64{
        self.counter
    }

    pub fn get_bytes(&self) -> ByteSize{
        self.byte_counter
    }
}

impl AddFileInfo for UpdateDB<'_>{
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {

        let write_txn = self.db.begin_write().map_err(Box::new)?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for (k,v) in files{
                self.counter+=1;
                match v{
                    FileMetadataExt::Dir(_) => {},
                    FileMetadataExt::File(file) => self.byte_counter.add_size(&file.size),
                    FileMetadataExt::Symlink(symlink) => self.byte_counter.add_size(&symlink.size),
                }

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
    counter: u64,
    byte_counter: ByteSize,
    pub files: HashSet<String>,
    compare_time: bool,
    changes_count: u64,
    new_files_count: u64,
}

impl<'ldb> CheckDB<'ldb>{
    pub fn new(db: &'ldb Database, compare_time: bool) -> Self{
        CheckDB { db, files: HashSet::new(), compare_time, counter: 0, byte_counter: ByteSize::default(), changes_count: 0, new_files_count: 0 }
    }

    pub fn get_counter(&self) -> u64{
        self.counter
    }

    pub fn get_bytes(&self) -> ByteSize{
        self.byte_counter
    }

    pub fn get_changes_count(&self) -> u64 {
        self.changes_count
    }

    pub fn get_new_files_count(&self) -> u64 {
        self.new_files_count
    }
}

impl AddFileInfo for CheckDB<'_> {
    fn add_file_info(&mut self, files: &[(String, FileMetadataExt)]) -> Result<(), IntegrityWatcherError> {

        let read_txn = self.db.begin_read().map_err(Box::new)?;
        let table = read_txn.open_table(TABLE)?;
        for (k, v) in files{
            self.files.insert(k.to_owned());

            self.counter += 1;

            match v{
                FileMetadataExt::Dir(_) => {},
                FileMetadataExt::File(file) => self.byte_counter.add_size(&file.size),
                FileMetadataExt::Symlink(symlink) => self.byte_counter.add_size(&symlink.size),
            }

            if let Some(oldv) = table.get(k)?{
                if oldv.value() != *v{
                    let old_val = oldv.value();
                    let mut info = String::new();

                    match (old_val, v)
                    {
                        (FileMetadataExt::Symlink(s), FileMetadataExt::File(f)) => {
                            error!("{} Symlink {} changed to file {}", k, s, f);
                            self.changes_count += 1;
                        },
                         (FileMetadataExt::File(f), FileMetadataExt::Symlink(s)) => {
                             error!("{} File {} changed to symlink {}", k, f, s);
                             self.changes_count += 1;
                         },
                         (FileMetadataExt::Dir(f), FileMetadataExt::Symlink(s)) => {
                             error!("{} Dir {} changed to symlink {}", k, f, s);
                             self.changes_count += 1;
                         },
                         (FileMetadataExt::Dir(f), FileMetadataExt::File(s)) => {
                             error!("{} Dir {} changed to file {}", k, f, s);
                             self.changes_count += 1;
                         },
                         (FileMetadataExt::Symlink(f), FileMetadataExt::Dir(s)) => {
                             error!("{} Symlink {} changed to dir {}", k, f, s);
                             self.changes_count += 1;
                         },
                         (FileMetadataExt::File(f), FileMetadataExt::Dir(s)) => {
                             error!("{} File {} changed to dir {}", k, f, s);
                             self.changes_count += 1;
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
                                self.changes_count += 1;
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
                                self.changes_count += 1;
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
                                self.changes_count += 1;
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
                self.new_files_count += 1;
            }
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FileMetadata, FileMetadataExt, Hash, ByteSize, DirMetadata, SymlinkMetadata};
    use redb::Database;
    use std::fs;

    fn setup_test_db(name: &str) -> (Database, std::path::PathBuf) {
        let mut path = std::env::current_dir().unwrap();
        path.push(format!("test_db_{}", name));
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();
        let db = Database::create(path.join("database.redb")).unwrap();
        (db, path)
    }

    #[test]
    fn test_write_to_db_logic() {
        let (db, path) = setup_test_db("write_logic");
        let mut writer = WriteToDB::new(&db);

        let hash = Hash::from([0u8; 32]);
        let file_meta = FileMetadataExt::File(FileMetadata {
            hash: hash.clone(),
            permissions: 0o644,
            modified: 123456789,
            size: ByteSize::new(1024),
        });

        let data = vec![
            ("file1.txt".to_string(), file_meta.clone()),
            ("file2.txt".to_string(), file_meta.clone()),
        ];

        writer.add_file_info(&data).unwrap();

        assert_eq!(writer.get_counter(), 2);
        assert_eq!(writer.get_bytes(), 2048.into());

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        assert!(table.get("file1.txt".to_owned()).unwrap().is_some());

        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_update_db_logic() {
        let (db, path) = setup_test_db("update_logic");

        {
            let mut writer = WriteToDB::new(&db);
            let hash = Hash::from([0u8; 32]);
            let file_meta = FileMetadataExt::File(FileMetadata {
                hash: hash.clone(),
                permissions: 0o644,
                modified: 123456789,
                size: ByteSize::new(1024),
            });
            writer.add_file_info(&[("file1.txt".to_string(), file_meta)]).unwrap();
        }

        let mut updater = UpdateDB::new(&db);
        let hash = Hash::from([1u8; 32]);
        let updated_meta = FileMetadataExt::File(FileMetadata {
            hash,
            permissions: 0o644,
            modified: 123456789,
            size: ByteSize::new(2048),
        });

        updater.add_file_info(&[("file1.txt".to_string(), updated_meta.clone())]).unwrap();

        assert_eq!(updater.get_counter(), 1);
        assert_eq!(updater.get_bytes(), 2048.into());
        assert!(updater.files.contains("file1.txt"));

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        let val = table.get("file1.txt".to_owned()).unwrap().unwrap();
        assert_eq!(val.value(), updated_meta);

        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_check_db_logic() {
        let (db, path) = setup_test_db("check_logic");

        {
            let mut writer = WriteToDB::new(&db);
            let hash = Hash::from([0u8; 32]);
            writer.add_file_info(&[
                ("file_hash".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 1000)),
                ("file_time".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 1000)),
                ("dir_size".to_string(), dir_metadata_helper(100, 1000)),
                ("sym_data".to_string(), symlink_metadata_helper("target", 10, 1000)),
                ("type_change".to_string(), dir_metadata_helper(100, 1000)),
            ]).unwrap();
        }

        {
            let mut checker = CheckDB::new(&db, false);
            let hash = Hash::from([0u8; 32]);
            let changed_hash = Hash::from([1u8; 32]);

            let files = vec![
                ("file_hash".to_string(), file_metadata_ext_helper(changed_hash, 1024, 1000)), // Hash change
                ("file_time".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 2000)), // Only time change
                ("dir_size".to_string(), dir_metadata_helper(200, 1000)), // Size change
                ("sym_data".to_string(), symlink_metadata_helper("new_target", 10, 1000)), // Data change
                ("type_change".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 1000)), // Type change
                ("new_file".to_string(), file_metadata_ext_helper(hash.clone(), 512, 1000)), // New file
            ];

            checker.add_file_info(&files).unwrap();

            // changes_count should be:
            // file_hash: yes
            // file_time: no (compare_time = false)
            // dir_size: yes
            // sym_data: yes
            // type_change: yes
            // new_file: no (it's a new file)
            assert_eq!(checker.get_changes_count(), 4);
            assert_eq!(checker.get_new_files_count(), 1);
        }

        {
            let mut checker = CheckDB::new(&db, true);
            let hash = Hash::from([0u8; 32]);
            let changed_hash = Hash::from([1u8; 32]);

            let files = vec![
                ("file_hash".to_string(), file_metadata_ext_helper(changed_hash, 1024, 1000)),
                ("file_time".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 2000)), // Now this is a change
                ("dir_size".to_string(), dir_metadata_helper(200, 1000)),
                ("sym_data".to_string(), symlink_metadata_helper("new_target", 10, 1000)),
                ("type_change".to_string(), file_metadata_ext_helper(hash.clone(), 1024, 1000)),
                ("new_file".to_string(), file_metadata_ext_helper(hash.clone(), 512, 1000)),
            ];

            checker.add_file_info(&files).unwrap();

            // changes_count should be 5 now (including file_time)
assert_eq!(checker.get_changes_count(), 5);
             assert_eq!(checker.get_new_files_count(), 1);
        }

        drop(db);
        fs::remove_dir_all(path).unwrap();
    }

    fn file_metadata_ext_helper(hash: Hash, size: u64, modified: u64) -> FileMetadataExt {
        FileMetadataExt::File(FileMetadata {
            hash,
            permissions: 0o644,
            modified,
            size: ByteSize::new(size),
        })
    }

    fn symlink_metadata_helper(data: &str, size: u64, modified: u64) -> FileMetadataExt {
        FileMetadataExt::Symlink(SymlinkMetadata {
            data: data.to_string(),
            permissions: 0o777,
            modified,
            size: ByteSize::new(size),
        })
    }

    fn dir_metadata_helper(size: u64, modified: u64) -> FileMetadataExt {
        FileMetadataExt::Dir(DirMetadata {
            permissions: 0o755,
            modified,
            size,
        })
    }
}
