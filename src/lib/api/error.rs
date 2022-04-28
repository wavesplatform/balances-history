#[derive(Debug, thiserror::Error)]

pub enum AppError {
    #[error("InvalidQueryString: {0}")]
    InvalidQueryString(String),

    #[error("DbError: {0}")]
    DbError(String),

    #[error("ValidationError: {0}")]
    ValidationError(String, Option<std::collections::HashMap<String, String>>),

    #[error("ValidationErrorCustom: {0}")]
    ValidationErrorCustom(String),
}
