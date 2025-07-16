#[derive(Debug, Clone)]
pub enum DeepLookOrderbookError {
    InternalError(String),
}

impl<E> From<E> for DeepLookOrderbookError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::InternalError(err.into().to_string())
    }
}
