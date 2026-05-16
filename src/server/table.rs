use std::fs::File;
use std::fmt;
use std::io::{BufReader, BufRead};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json;
use crate::server::storage::KVError;


#[derive(Deserialize, Serialize)]
pub struct Entry {
    key: String,
    value: String,
    deleted: bool
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Info {
    pub value: String,
    pub deleted: bool
}

#[derive(Clone)]
pub struct Memtable {
    table: HashMap<String, Info>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            table: HashMap::<String, Info>::new()
        }
    }

    pub fn get(&self, key: &str) -> Result<String, KVError> {
        match self.table.get(key) {
            Some(info) => {
                if info.deleted {
                    Err(KVError::Deleted)
                } else {
                    Ok(info.value.to_string())
                }
            }
            None => Err(KVError::NotFound),
        }
    }

    pub fn insert(&mut self, entry: Entry) {
        self.table.insert(entry.key, Info {value: entry.value, deleted: entry.deleted});
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }

    pub fn len(&self) -> usize {
        return self.table.len();
    }
}

pub struct MemtableIter {
    entries: Vec<(String, Info)>,
    pos: usize,
}

impl MemtableIter {
    pub fn new(memtable: &Memtable) -> Self {
        let mut entries: Vec<(String, Info)> = memtable.table
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Self { entries, pos: 0 }
    }
}

impl Iterator for MemtableIter {
    type Item = (String, Info);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let item = self.entries[self.pos].clone();
        self.pos += 1;
        Some(item)
    }
}

pub struct SSTableIter {
    reader: BufReader<File>
}

impl SSTableIter {
    pub fn new(path: &str) -> Self {
        let file = File::open(path).unwrap();
        Self { reader: BufReader::new(file) }
    }
}

impl Iterator for SSTableIter {
    type Item = (String, Info);

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => {
                let v: serde_json::Value = serde_json::from_str(&line).unwrap();
                let key = v["key"].as_str().unwrap().to_string();
                let value = v["value"].as_str().unwrap().to_string();
                let deleted = v["deleted"].as_bool().unwrap();
                Some((key, Info {value, deleted}))
            }
            Err(_) => None,
        }
    }
}

impl Entry {
    pub fn new(key: String, info: Info) -> Self {
        Self {key: key, value: info.value, deleted: info.deleted}
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use write! to send the formatted string to the formatter 'f'
        write!(f, "{}", serde_json::json!({"key": self.key, "value": self.value, "deleted": self.deleted}))
    }
}