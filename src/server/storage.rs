use regex::Regex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct KVStore {
    memtable: HashMap<String, String>,
    next_id: u64,
    manifest: Arc<Mutex<File>>,
    data_dir: String,
}

impl KVStore {
    fn get_next_id(manifest_path: &str) -> u64 {
        let mut next_id: u64 = 0;
        let file = File::open(manifest_path).unwrap();
        let reader = BufReader::new(file);

        if let Some(last_line) = reader.lines().last() {
            let re = Regex::new(r"^(?P<name>[a-zA-Z0-9]+)-(?P<num>\d+)\.json$").unwrap();

            if let Some(caps) = re.captures(&last_line.unwrap()) {
                let num = &caps["num"];
                next_id = num.parse::<u64>().expect("Not a valid number") + 1;
            } else {
                eprintln!("Line did not match format");
            }
        }
        next_id
    }

    pub fn new(data_dir: String) -> Self {
        let manifest_path = format!("{}/MANIFEST.txt", data_dir);

        let mut next_id: u64 = 0;
        if !fs::exists(&manifest_path).unwrap() {
            let file = File::create(&manifest_path).unwrap();
            drop(file);
        } else {
            next_id = Self::get_next_id(&manifest_path);
        }

        let manifest = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&manifest_path)
            .unwrap();

        Self {
            memtable: HashMap::new(),
            next_id,
            manifest: Arc::new(Mutex::new(manifest)),
            data_dir,
        }
    }

    pub fn get(&mut self, key: &str) -> Result<String, String> {
        if let Ok(value) = self.search_memtable(key) {
            return Ok(value);
        }
        if let Ok(value) = self.search_sstables(key) {
            return Ok(value);
        }
        Err(format!("Key {key} not found"))
    }

    fn search_memtable(&self, key: &str) -> Result<String, String> {
        match self.memtable.get(key) {
            Some(value) => Ok(value.to_string()),
            None => Err(format!("Key {key} not found")),
        }
    }

    fn search_sstables(&self, key: &str) -> Result<String, String> {
        // Open a fresh read handle instead of seeking the shared append handle
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let file = File::open(&manifest_path).unwrap();
        let reader = BufReader::new(file);
        let mut lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
        lines.reverse();

        for line in lines.iter() {
            if let Ok(value) = self.search_sstable(&line, key) {
                return Ok(value);
            }
        }
        Err(format!("Key {key} not found"))
    }

    fn search_sstable(&self, sstable_name: &str, key: &str) -> Result<String, String> {
        let path = format!("{}/{}", self.data_dir, sstable_name);
        let data = fs::read_to_string(&path).unwrap();
        let map: HashMap<String, String> = serde_json::from_str(&data).unwrap();

        match map.get(key) {
            Some(value) => Ok(value.to_string()),
            None => Err(format!("Key {key} not found")),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);

        if self.memtable.len() == 2000 {
            self.flush();
            self.memtable.clear();
        }
    }

    fn flush(&mut self) {
        let json = serde_json::to_string(&self.memtable).unwrap();
        let sst_name = format!("sst-{}.json", self.next_id);
        let sst_path = format!("{}/{}", self.data_dir, sst_name);

        fs::write(&sst_path, json).unwrap();

        let mut manifest = self.manifest.lock().unwrap();
        writeln!(manifest, "{}", sst_name).unwrap();

        self.next_id += 1;
    }
}
