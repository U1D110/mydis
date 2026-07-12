pub enum Poll<T> {
    Ready(T),
    Pending,
}