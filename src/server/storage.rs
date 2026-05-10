use std::collections::HashMap;

#[derive(Clone)]
pub struct KVStore {
    map: HashMap<String, String>,
}

impl KVStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn get(&self, key: &str) -> Result<&String, String> {
        match self.map.get(key) {
            Some(value) => Ok(value),
            None => Err(String::from("Key {key} not found")),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
}
