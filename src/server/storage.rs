use libc;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

#[derive(Clone)]
pub struct KVStore {
    memtable: HashMap<String, String>,
    next_id: u64,
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

    fn replay_wal(wal_path: &str, memtable: &mut HashMap<String, String>) {
        let file = File::open(wal_path).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();
            let v: Value = serde_json::from_str(&line).unwrap();
            let key = v["key"].as_str().unwrap();
            let value = v["value"].as_str().unwrap();
            memtable.insert(key.to_string(), value.to_string());
        }
    }

    pub fn new(data_dir: String) -> Self {
        let manifest_path = format!("{}/MANIFEST.txt", data_dir);
        let wal_path = format!("{}/wal.db", data_dir);
        let mut memtable = HashMap::<String, String>::new();

        let mut next_id: u64 = 0;
        if !fs::exists(&manifest_path).unwrap() {
            let file = File::create(&manifest_path).unwrap();
            drop(file);
        } else {
            next_id = Self::get_next_id(&manifest_path);
        }

        if !fs::exists(&wal_path).unwrap() {
            let file = File::create(&wal_path).unwrap();
            drop(file);
        } else {
            Self::replay_wal(&wal_path, &mut memtable);
        }

        Self {
            memtable,
            next_id,
            data_dir,
        }
    }

    pub fn get(&self, key: &str) -> Result<String, String> {
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
        {
            let entry = serde_json::json!({"op": "put", "key": key, "value": value}).to_string();
            let wal_path = format!("{}/wal.db", self.data_dir);
            let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();

            writeln!(wal, "{}", entry).unwrap();
            wal.sync_all().unwrap();
        }

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

        // 1. Write and fsync the SSTable
        let mut sst_file: File = File::create(&sst_path).unwrap();
        sst_file.write_all(json.as_bytes()).unwrap();
        sst_file.sync_all().unwrap();

        // 2. Atomically update the manifest via tmp file
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let tmp_path = format!("{}/MANIFEST.tmp", self.data_dir);

        fs::copy(&manifest_path, &tmp_path).unwrap();

        let mut tmp = OpenOptions::new().append(true).open(&tmp_path).unwrap();
        writeln!(tmp, "{}", sst_name).unwrap();
        tmp.sync_all().unwrap();
        drop(tmp);

        fs::rename(&tmp_path, &manifest_path).unwrap();
        self.fsync_parent_dir(Path::new(&manifest_path)).unwrap();
        self.next_id += 1;

        // 3. Truncate and fsync the WAL now that flush is durable
        let wal_path = format!("{}/wal.db", self.data_dir);
        let wal = File::create(&wal_path).unwrap();
        wal.sync_all().unwrap();
    }

    fn fsync_parent_dir(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            let dir = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY)
                .open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }
}
