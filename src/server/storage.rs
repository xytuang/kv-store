use libc;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashSet, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use crate::server::table::{Memtable, MemtableIter, SSTableIter, Entry, Info};

#[derive(Debug)]
pub enum KVError {
    NotFound,
    Deleted,
}

#[derive(Clone)]
pub struct KVStore {
    memtable: Memtable,
    next_id: u64,
    data_dir: String,
    update_count: u64
}

// HeapEntry struct specifically for compaction
struct HeapEntry {
    key: String,
    age: usize,
    iter_idx: usize,
    info: Info
}

// Enables equality and inequality comparisons. PartialEq does not require reflextivity
// PartialEq must be implemented before Eq as Eq is a subtrait of PartialEq
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool { self.key == other.key && self.age == other.age }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for HeapEntry {
    // VERY CURSED SEMANTICS
    // Heap pop element that cmp considers the greatest
    // When we do heap.pop on two entries A and B, we are doing A.cmp(&B) ie. A is self, B is other
    // For A to pop first, A must be considered Greatest.
    // This means that A.cmp(&B) must return Greater
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by key, break ties by age (lower = newer = higher priority)
        other.key.cmp(&self.key).then(other.age.cmp(&self.age))
    }
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

    fn replay_wal(wal_path: &str, memtable: &mut Memtable) {
        let file = File::open(wal_path).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();
            let v: Value = serde_json::from_str(&line).unwrap();
            let op = v["op"].as_str().unwrap();
            let key = v["key"].as_str().unwrap();

            if op == "put" {
                let value = v["value"].as_str().unwrap();
                memtable.insert(Entry::new(key.to_string(), Info {value: value.to_string(), deleted: false}));
            } else {
                memtable.insert(Entry::new(key.to_string(), Info {value: "0".to_string(), deleted: true}));
            }
        }
    }

    pub fn new(data_dir: String) -> Self {
        let manifest_path = format!("{}/MANIFEST.txt", data_dir);
        let wal_path = format!("{}/wal.db", data_dir);
        let mut memtable = Memtable::new();

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
            update_count: 0
        }
    }

    pub fn get(&self, key: &str) -> Result<String, String> {
        match self.memtable.get(key) {
            Ok(value) => return Ok(value),
            Err(KVError::Deleted) => return  Err(format!("Key {key} not found")),
            _ => ()
        }
        if let Ok(value) = self.search_sstables(key) {
            return Ok(value);
        }
        Err(format!("Key {key} not found"))
    }

    fn search_sstables(&self, key: &str) -> Result<String, String> {
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let file = File::open(&manifest_path).unwrap();
        let reader = BufReader::new(file);
        let mut lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
        lines.reverse();

        for line in lines.iter() {
            match self.search_sstable(&line, key) {
                Ok(value) => return Ok(value),
                Err(KVError::Deleted) => return Err(format!("Key {key} not found")), // stop searching
                Err(KVError::NotFound) => continue, // key not in this SSTable, keep looking
            }
        }
        Err(format!("Key {key} not found"))
    }

    fn search_sstable(&self, sstable_name: &str, key: &str) -> Result<String, KVError> {
        let path = format!("{}/{}", self.data_dir, sstable_name);

        // Brute force linear scan over all keys. Can be optimized
        for (k, info) in SSTableIter::new(&path) {
            if key == k {
                if info.deleted { return Err(KVError::Deleted); } else { return Ok(info.value); }
            }
        }
        Err(KVError::NotFound)
    }

    pub fn put(&mut self, key: String, value: String) {
        {
            let entry = serde_json::json!({"op": "put", "key": key, "value": value}).to_string();
            let wal_path = format!("{}/wal.db", self.data_dir);
            let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();

            writeln!(wal, "{}", entry).unwrap();
            wal.sync_all().unwrap();
        }
        self.memtable.insert(Entry::new(key, Info{value: value, deleted: false}));

        if self.memtable.len() == 2000 {
            self.flush();
            self.memtable.clear();
        }

        self.update_count += 1;
        if self.update_count == 10000 {
            if self.memtable.len() > 0 {
                self.flush();
                self.memtable.clear();
            }
            self.compact();
            self.update_count = 0;
        }
    }

    fn flush(&mut self) {
        let sst_name = format!("sst-{}.json", self.next_id);
        let sst_path = format!("{}/{}", self.data_dir, sst_name);

        // 1. Write and fsync the SSTable
        let mut sst_file: File = File::create(&sst_path).unwrap();

        // MemtableIter sorts the keys
        for (key, info) in MemtableIter::new(&self.memtable) {
            let e = Entry::new(key, info);
            let line = e.to_string();
            writeln!(sst_file, "{}", line).unwrap();
        }
    
        sst_file.sync_all().unwrap();
        self.fsync_parent_dir(Path::new(&sst_path)).unwrap();

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

    fn compact(&mut self) {
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let file = File::open(&manifest_path).unwrap();
        let reader = BufReader::new(file);
        let mut lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
        lines.reverse(); // newest first

        // Create array of iterators.
        let mut iters: Vec<Box<dyn Iterator<Item = (String, Info)>>> = vec![];

        // Add iterators for SSTables in most recent order
        for line in lines.iter() {
            let path = format!("{}/{}", self.data_dir, line);
            iters.push(Box::new(SSTableIter::new(&path)));
        }

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        // Add the first key of each iterator into the heap
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, info)) = iter.next() {
                heap.push(HeapEntry {key: key, age: idx, iter_idx: idx, info: info});
            }
        }

        // K way merge
        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut output: Vec<(String, Info)> = Vec::new();

        while let Some(heap_entry) = heap.pop() {
            if let Some((next_key, next_info)) = iters[heap_entry.iter_idx].next() {
                heap.push(HeapEntry {
                    key: next_key,
                    info: next_info,
                    iter_idx: heap_entry.iter_idx,
                    age: heap_entry.age
                });
            }

            // Skip older duplicates
            if seen_keys.contains(&heap_entry.key) { continue; }
            seen_keys.insert(heap_entry.key.clone());
            if heap_entry.info.deleted { continue; }

            output.push((heap_entry.key, heap_entry.info));
        }

        // Stream merged table to new SSTables
        output.sort_by_key(|k| k.0.clone());

        let new_beginning_id = self.next_id; // Get the id of the first SSTable for new manifest
        for chunk in output.chunks(2000) {
            // Write single compacted SSTable
            let sst_name = format!("sst-{}.json", self.next_id);
            let sst_path = format!("{}/{}", self.data_dir, sst_name);
            let mut sst_file = File::create(&sst_path).unwrap();

            for (key, info) in chunk {
                let e = Entry::new(key.to_string(), info.clone());
                let line = e.to_string();
                writeln!(sst_file, "{}", line).unwrap();
            }
            sst_file.sync_all().unwrap();
            self.fsync_parent_dir(Path::new(&sst_path)).unwrap();

            self.next_id += 1;
        }

        // Rewrite manifest with just the compacted SSTable
        let tmp_path = format!("{}/MANIFEST.tmp", self.data_dir);
        let mut tmp = File::create(&tmp_path).unwrap();
        for i in new_beginning_id..self.next_id {
            let sst_name = format!("sst-{}.json", i);
            writeln!(tmp, "{}", sst_name).unwrap();
        }
        tmp.sync_all().unwrap();
        drop(tmp);

        fs::rename(&tmp_path, &manifest_path).unwrap();
        self.fsync_parent_dir(Path::new(&manifest_path)).unwrap();

        // Delete old SSTable files
        for line in lines.iter() {
            let path = format!("{}/{}", self.data_dir, line);
            fs::remove_file(&path).unwrap();
        }
    }

    pub fn delete(&mut self, key: &str) {
        let entry = serde_json::json!({"op": "delete", "key": key}).to_string();
        let wal_path = format!("{}/wal.db", self.data_dir);
        let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();

        writeln!(wal, "{}", entry).unwrap();
        wal.sync_all().unwrap();

        self.memtable.insert(Entry::new(key.to_string(), Info {value: "0".to_string(), deleted: true}));

        self.update_count += 1;

        if self.update_count == 10000 {
            if self.memtable.len() > 0 {
                self.flush();
                self.memtable.clear();
            }
            self.compact();
            self.update_count = 0;
        }
    }
}
