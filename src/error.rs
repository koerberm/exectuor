use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, ExecutorError>;

#[derive(Debug, Clone)]
pub enum ExecutorError {
    TaskSubmission { message: String },
    TaskPanic,
    TaskCancelled,
}

impl From<JoinError> for ExecutorError {
    fn from(src: JoinError) -> Self {
        if src.is_cancelled() {
            ExecutorError::TaskCancelled
        } else {
            ExecutorError::TaskPanic
        }
    }
}

impl<T> From<SendError<T>> for ExecutorError {
    fn from(e: SendError<T>) -> Self {
        Self::TaskSubmission {
            message: e.to_string(),
        }
    }
}

impl From<RecvError> for ExecutorError {
    fn from(e: RecvError) -> Self {
        Self::TaskSubmission {
            message: e.to_string(),
        }
    }
}
