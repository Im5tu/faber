use tonic::Code;

#[derive(Debug, thiserror::Error)]
pub enum CriError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("runtime unavailable: {0}")]
    Unavailable(String),
    #[error("cri call failed ({code:?}): {message}")]
    Other { code: Code, message: String },
    #[error("connecting to the runtime: {0}")]
    Connect(String),
    #[error("the CRI client needs a Unix socket and is unsupported on this platform")]
    Unsupported,
    #[error("runtime response missing {0}")]
    MalformedResponse(&'static str),
}

impl CriError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }
}

impl From<tonic::Status> for CriError {
    fn from(status: tonic::Status) -> Self {
        let message = status.message().to_owned();
        match status.code() {
            Code::NotFound => Self::NotFound(message),
            Code::AlreadyExists => Self::AlreadyExists(message),
            Code::InvalidArgument => Self::InvalidArgument(message),
            Code::FailedPrecondition => Self::FailedPrecondition(message),
            Code::Unavailable => Self::Unavailable(message),
            code => Self::Other { code, message },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_status_codes() {
        let cases = [
            (tonic::Status::not_found("x"), "not found: x"),
            (tonic::Status::already_exists("x"), "already exists: x"),
            (tonic::Status::invalid_argument("x"), "invalid argument: x"),
            (
                tonic::Status::failed_precondition("x"),
                "failed precondition: x",
            ),
            (tonic::Status::unavailable("x"), "runtime unavailable: x"),
            (
                tonic::Status::internal("x"),
                "cri call failed (Internal): x",
            ),
        ];
        for (status, text) in cases {
            assert_eq!(CriError::from(status).to_string(), text);
        }
        assert!(CriError::from(tonic::Status::not_found("x")).is_not_found());
        assert!(!CriError::from(tonic::Status::internal("x")).is_not_found());
    }
}
