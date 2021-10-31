#[derive(Debug, thiserror::Error)]

pub enum AppError {
    #[error("InvalidQueryString: {0}")]
    InvalidQueryString(String),
    #[error("DbError: {0}")]
    DbError(String),
}
