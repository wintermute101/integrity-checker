use thiserror::Error;

#[derive(Error, Debug)]
pub enum IntegrityWatcherError {
    #[error("IO error {source} file {path}")]
    IOError{
        #[source]
        source: std::io::Error,
        path: String,
    },

    #[error("Join error {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("Aquire error {0}")]
    Aquire(#[from] tokio::sync::AcquireError),

    #[error("DB error {0}")]
    DB(#[from] redb::DatabaseError),

    #[error("DB Storage error {0}")]
    DBStorage(#[from] redb::StorageError),

    #[error("DB Transaction error {0}")]
    DBTransaction(#[from] Box<redb::TransactionError>),

    #[error("DB Table error {0}")]
    DBTable(#[from] redb::TableError),

    #[error("DB Commit error {0}")]
    DBCommit(#[from] redb::CommitError),

    #[error("SystemTimeError error {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("Reqwest error {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Invalid response {status} in hash {hash}")]
    InvalidReponse{
        status: u16,
        hash: super::types::Hash
    }
}