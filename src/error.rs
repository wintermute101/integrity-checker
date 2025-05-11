use thiserror::Error;

#[derive(Error, Debug)]
pub enum ItegrityWatcherError {
    #[error("IO error {source} file {path}")]
    IOError{
        #[source]
        source: std::io::Error,
        path: String,
    },

    #[error("Join error {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("DB error {0}")]
    DB(#[from] redb::DatabaseError),

    #[error("DB Storage error {0}")]
    DBStorage(#[from] redb::StorageError),

    #[error("DB Transaction error {0}")]
    DBTransaction(#[from] redb::TransactionError),

    #[error("DB Table error {0}")]
    DBTable(#[from] redb::TableError),

    #[error("DB Commit error {0}")]
    DBCommit(#[from] redb::CommitError),

    #[error("SystemTimeError error {0}")]
    SystemTime(#[from] std::time::SystemTimeError)

}