// 0	timeout		Indicates that the requested operation could not be completed within a timeout.
// 1	node-not-found	✓	Thrown when a client sends an RPC request to a node which does not exist.
// 10	not-supported	✓	Use this error to indicate that a requested operation is not supported by the current implementation. Helpful for stubbing out APIs during development.
// 11	temporarily-unavailable	✓	Indicates that the operation definitely cannot be performed at this time--perhaps because the server is in a read-only state, has not yet been initialized, believes its peers to be down, and so on. Do not use this error for indeterminate cases, when the operation may actually have taken place.
// 12	malformed-request	✓	The client's request did not conform to the server's expectations, and could not possibly have been processed.
// 13	crash		Indicates that some kind of general, indefinite error occurred. Use this as a catch-all for errors you can't otherwise categorize, or as a starting point for your error handler: it's safe to return internal-error for every problem by default, then add special cases for more specific errors later.
// 14	abort	✓	Indicates that some kind of general, definite error occurred. Use this as a catch-all for errors you can't otherwise categorize, when you specifically know that the requested operation has not taken place. For instance, you might encounter an indefinite failure during the prepare phase of a transaction: since you haven't started the commit process yet, the transaction can't have taken place. It's therefore safe to return a definite abort to the client.
// 20	key-does-not-exist	✓	The client requested an operation on a key which does not exist (assuming the operation should not automatically create missing keys).
// 21	key-already-exists	✓	The client requested the creation of a key which already exists, and the server will not overwrite it.
// 22	precondition-failed	✓	The requested operation expected some conditions to hold, and those conditions were not met. For instance, a compare-and-set operation might assert that the value of a key is currently 5; if the value is 3, the server would return precondition-failed.
// 30	txn-conflict	✓	The requested transaction has been aborted because of a conflict with another transaction. Servers need not return this error on every conflict: they may choose to retry automatically instead.

#[derive(Debug, Eq, PartialEq)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TransactionConflict = 30
}

impl From<u64> for ErrorCode {
    fn from(value: u64) -> Self {
        match value {
            0 => ErrorCode::Timeout,
            1 => ErrorCode::NodeNotFound,
            10 => ErrorCode::NotSupported,
            11 => ErrorCode::TemporarilyUnavailable,
            12 => ErrorCode::MalformedRequest,
            13 => ErrorCode::Crash,
            14 => ErrorCode::Abort,
            20 => ErrorCode::KeyDoesNotExist,
            21 => ErrorCode::KeyAlreadyExists,
            22 => ErrorCode::PreconditionFailed,
            30 => ErrorCode::TransactionConflict,
            _ => panic!("invalid error code: {}", value),
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub text: String,
}
