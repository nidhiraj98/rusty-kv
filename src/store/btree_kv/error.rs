#[derive(Eq, PartialEq, Debug)]
pub enum RustyKVError {
    InsufficientSpace,
    ItemNotFound,
    UnknownError,
}
