

// Abstraction over Key-Value storage, with next assumptions:
// 1. Empty value is same as None.
// 2. remove by default use `set` with empty slice.
// 3. Any operation can fail.
// 4. 
trait Database {
    type Error;

    fn get(&self, key: &[u8]) -> Result<Cow<'a>, Error>;
}

trait DatabaseMut {
    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    
    fn remove(&self, key: &[u8]) -> Result<(), Error> {
        fn
    }
}