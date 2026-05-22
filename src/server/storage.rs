use crate::server::table::{Entry, Info, Memtable, MemtableIter, SSTableIter};
use libc;
use regex::Regex;
use serde_json::Value;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

#[derive(Debug)]
pub enum KVError {
    NotFound,
    Deleted,
}

#[derive(Clone)]
pub struct KVStore {
    memtable: Memtable,
    next_ids: Vec<usize>,
    data_dir: String,
}

#[derive(Clone)]
struct ManifestLine {
    start_key: String,
    end_key: String,
    sst_name: String,
}

struct HeapEntry {
    key: String,
    age: usize,
    iter_idx: usize,
    info: Info,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.age == other.age
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap by key, break ties by age (lower = newer = higher priority)
        other.key.cmp(&self.key).then(other.age.cmp(&self.age))
    }
}

impl KVStore {
    fn get_next_ids(manifest_path: &str, next_ids: &mut Vec<usize>) {
        let file = File::open(manifest_path).unwrap();
        let reader = BufReader::new(file);
        let sst_regex = Regex::new(r"^(?P<name>[a-zA-Z0-9]+)-(?P<num>\d+)\.json$").unwrap();
        let level_regex = Regex::new(r"^\[L\d+\]$").unwrap();
        let mut in_level = false;
        let mut next_id: usize = 0;

        for line in reader.lines() {
            let line = line.unwrap();
            if level_regex.is_match(&line) {
                if in_level {
                    // Push the previous level's next_id before starting a new level
                    next_ids.push(next_id);
                }
                next_id = 0;
                in_level = true;
            } else if let Some(caps) = sst_regex.captures(&line) {
                let num = &caps["num"];
                next_id = num.parse::<usize>().expect("Not a valid number") + 1;
            }
        }
        // Push the last level's next_id
        if in_level {
            next_ids.push(next_id);
        }
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
                memtable.insert(Entry::new(
                    key.to_string(),
                    Info {
                        value: value.to_string(),
                        deleted: false,
                    },
                ));
            } else {
                memtable.insert(Entry::new(
                    key.to_string(),
                    Info {
                        value: "0".to_string(),
                        deleted: true,
                    },
                ));
            }
        }
    }

    pub fn new(data_dir: String) -> Self {
        let manifest_path = format!("{}/MANIFEST.txt", data_dir);
        let wal_path = format!("{}/wal.db", data_dir);
        let l0_path = format!("{}/l0", data_dir);
        let mut memtable = Memtable::new();

        let mut next_ids: Vec<usize> = Vec::new();
        if !fs::exists(&manifest_path).unwrap() {
            let mut file = File::create(&manifest_path).unwrap();
            writeln!(file, "[L0]").unwrap();
            drop(file);
            next_ids.push(0);
        } else {
            Self::get_next_ids(&manifest_path, &mut next_ids);
        }

        // Create level directories that don't exist yet
        for i in 0..next_ids.len() {
            let dir = format!("{}/l{}", data_dir, i);
            if !fs::exists(&dir).unwrap() {
                fs::create_dir(&dir).unwrap();
            }
        }

        // Always ensure l0 exists
        if !fs::exists(&l0_path).unwrap() {
            fs::create_dir(&l0_path).unwrap();
        }

        if !fs::exists(&wal_path).unwrap() {
            let file = File::create(&wal_path).unwrap();
            drop(file);
        } else {
            Self::replay_wal(&wal_path, &mut memtable);
        }

        Self {
            memtable,
            next_ids,
            data_dir,
        }
    }

    pub fn get(&self, key: &str) -> Result<String, String> {
        match self.memtable.get(key) {
            Ok(value) => return Ok(value),
            Err(KVError::Deleted) => return Err(format!("Key {key} not found")),
            _ => (),
        }
        if let Ok(value) = self.search_sstables(key) {
            return Ok(value);
        }
        Err(format!("Key {key} not found"))
    }

    fn search_sstables(&self, key: &str) -> Result<String, String> {
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let all_sst_filenames = self.parse_manifest(&manifest_path);

        // Search L0 first (newest), then deeper levels
        for (level, level_files) in all_sst_filenames.iter().enumerate() {
            // Within a level, search newest SSTable first
            for manifest_line in level_files.iter().rev() {
                if level == 0 {
                    let path = format!("{}/l{}/{}", self.data_dir, level, manifest_line.sst_name);
                    match self.search_sstable(&path, key) {
                        Ok(value) => return Ok(value),
                        Err(KVError::Deleted) => return Err(format!("Key {key} not found")),
                        Err(KVError::NotFound) => continue,
                    }
                } else if *key >= *manifest_line.start_key && *key <= *manifest_line.end_key {
                    let path = format!("{}/l{}/{}", self.data_dir, level, manifest_line.sst_name);
                    match self.search_sstable(&path, key) {
                        Ok(value) => return Ok(value),
                        Err(KVError::Deleted) => return Err(format!("Key {key} not found")),
                        Err(KVError::NotFound) => continue,
                    }
                }
            }
        }

        Err(format!("Key {key} not found"))
    }

    fn search_sstable(&self, path: &str, key: &str) -> Result<String, KVError> {
        for (k, info) in SSTableIter::new(path) {
            if key == k {
                if info.deleted {
                    return Err(KVError::Deleted);
                } else {
                    return Ok(info.value);
                }
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
        self.memtable.insert(Entry::new(
            key,
            Info {
                value: value,
                deleted: false,
            },
        ));

        if self.memtable.len() == 2000 {
            self.flush();
            self.memtable.clear();
        }
    }

    fn flush(&mut self) {
        let sst_name = format!("sst-{}.json", self.next_ids[0]);
        let sst_path = format!("{}/l0/{}", self.data_dir, sst_name);

        // 1. Write and fsync the SSTable
        let mut sst_file: File = File::create(&sst_path).unwrap();
        for (key, info) in MemtableIter::new(&self.memtable) {
            let e = Entry::new(key, info);
            writeln!(sst_file, "{}", e).unwrap();
        }
        sst_file.sync_all().unwrap();
        self.fsync_parent_dir(Path::new(&sst_path)).unwrap();

        // 2. Atomically update the manifest via tmp file
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let tmp_path = format!("{}/MANIFEST.tmp", self.data_dir);

        let mut tmp = File::create(&tmp_path).unwrap();
        let mut all_sst_filenames = self.parse_manifest(&manifest_path);
        all_sst_filenames[0].push(ManifestLine {
            start_key: "NA".to_string(),
            end_key: "NA".to_string(),
            sst_name: sst_name,
        });

        for (i, level_files) in all_sst_filenames.iter().enumerate() {
            writeln!(tmp, "[L{}]", i).unwrap();
            for manifest_line in level_files {
                if i == 0 {
                    writeln!(tmp, "{}", manifest_line.sst_name).unwrap();
                } else {
                    let line = format!(
                        "{}-{}:{}",
                        manifest_line.start_key, manifest_line.end_key, manifest_line.sst_name
                    );
                    writeln!(tmp, "{}", line).unwrap();
                }
            }
        }
        tmp.sync_all().unwrap();
        drop(tmp);

        fs::rename(&tmp_path, &manifest_path).unwrap();
        self.fsync_parent_dir(Path::new(&manifest_path)).unwrap();
        self.next_ids[0] += 1;

        // 3. Truncate and fsync the WAL
        let wal_path = format!("{}/wal.db", self.data_dir);
        let wal = File::create(&wal_path).unwrap();
        wal.sync_all().unwrap();

        self.compact();
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
        let mut all_sst_filenames = self.parse_manifest(&manifest_path);
        self.compact_level(0, &mut all_sst_filenames);
    }

    fn compact_level(&mut self, level: usize, all_sst_filenames: &mut Vec<Vec<ManifestLine>>) {
        if level >= all_sst_filenames.len() || all_sst_filenames[level].len() < 5 {
            return;
        }

        // Snapshot the filenames to compact and clear the level
        let sst_filenames: Vec<ManifestLine> = all_sst_filenames[level].clone();
        let mut sorted_filenames = sst_filenames.clone();
        sorted_filenames.reverse(); // newest first
        all_sst_filenames[level].clear();

        // Build iterators for each SSTable in this level
        let mut iters: Vec<Box<dyn Iterator<Item = (String, Info)>>> = vec![];
        for manifest_line in sorted_filenames.iter() {
            let path = format!("{}/l{}/{}", self.data_dir, level, manifest_line.sst_name);
            iters.push(Box::new(SSTableIter::new(&path)));
        }

        // Seed the heap
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, info)) = iter.next() {
                heap.push(HeapEntry {
                    key,
                    age: idx,
                    iter_idx: idx,
                    info,
                });
            }
        }

        // K-way merge
        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut output: Vec<(String, Info)> = Vec::new();

        while let Some(heap_entry) = heap.pop() {
            if let Some((next_key, next_info)) = iters[heap_entry.iter_idx].next() {
                heap.push(HeapEntry {
                    key: next_key,
                    info: next_info,
                    iter_idx: heap_entry.iter_idx,
                    age: heap_entry.age,
                });
            }

            if seen_keys.contains(&heap_entry.key) {
                continue;
            }
            seen_keys.insert(heap_entry.key.clone());
            if heap_entry.info.deleted {
                continue;
            }

            output.push((heap_entry.key, heap_entry.info));
        }

        output.sort_by(|a, b| a.0.cmp(&b.0));

        // Ensure next level exists
        let next_level = level + 1;
        if self.next_ids.len() <= next_level {
            self.create_new_level();
            all_sst_filenames.push(Vec::new());
        }

        // Write compacted output to next level
        let mut next_level_sst_filenames: Vec<ManifestLine> = Vec::new();
        for chunk in output.chunks(2000) {
            let sst_name = format!("sst-{}.json", self.next_ids[next_level]);
            let sst_path = format!("{}/l{}/{}", self.data_dir, next_level, sst_name);
            let mut sst_file = File::create(&sst_path).unwrap();

            for (key, info) in chunk {
                let e = Entry::new(key.to_string(), info.clone());
                writeln!(sst_file, "{}", e).unwrap();
            }
            sst_file.sync_all().unwrap();
            self.fsync_parent_dir(Path::new(&sst_path)).unwrap();

            self.next_ids[next_level] += 1;
            next_level_sst_filenames.push(ManifestLine {
                start_key: chunk[0].0.to_string(),
                end_key: chunk[chunk.len() - 1].0.to_string(),
                sst_name: sst_name,
            });
        }

        // Append new files to existing next level list
        all_sst_filenames[next_level].extend(next_level_sst_filenames);

        // Delete old SSTable files from this level
        for manifest_line in &sst_filenames {
            let path = format!("{}/l{}/{}", self.data_dir, level, manifest_line.sst_name);
            fs::remove_file(&path).unwrap();
        }

        // Atomically rewrite manifest
        let manifest_path = format!("{}/MANIFEST.txt", self.data_dir);
        let tmp_path = format!("{}/MANIFEST.tmp", self.data_dir);
        let mut tmp = File::create(&tmp_path).unwrap();
        for (i, level_files) in all_sst_filenames.iter().enumerate() {
            writeln!(tmp, "[L{}]", i).unwrap();
            for manifest_line in level_files {
                if i == 0 {
                    writeln!(tmp, "{}", manifest_line.sst_name).unwrap();
                } else {
                    let line = format!(
                        "{}-{}:{}",
                        manifest_line.start_key, manifest_line.end_key, manifest_line.sst_name
                    );
                    writeln!(tmp, "{}", line).unwrap();
                }
            }
        }
        tmp.sync_all().unwrap();
        drop(tmp);

        fs::rename(&tmp_path, &manifest_path).unwrap();
        self.fsync_parent_dir(Path::new(&manifest_path)).unwrap();

        // Recurse to compact next level if needed
        self.compact_level(next_level, all_sst_filenames);
    }

    fn parse_manifest(&self, manifest_path: &str) -> Vec<Vec<ManifestLine>> {
        let file = File::open(manifest_path).unwrap();
        let reader = BufReader::new(file);
        let level_regex = Regex::new(r"^\[L\d+\]$").unwrap();
        let mut all_sst_filenames: Vec<Vec<ManifestLine>> = Vec::new();

        for line in reader.lines() {
            let line = line.unwrap();
            if level_regex.is_match(&line) {
                all_sst_filenames.push(Vec::new());
            } else if !line.trim().is_empty() {
                if all_sst_filenames.len() > 1 {
                    let parts: Vec<&str> = line.split(':').collect();
                    let keys: Vec<&str> = parts[0].split('-').collect();

                    all_sst_filenames.last_mut().unwrap().push(ManifestLine {
                        start_key: keys[0].to_string(),
                        end_key: keys[1].to_string(),
                        sst_name: parts[1].to_string(),
                    });
                } else {
                    all_sst_filenames.last_mut().unwrap().push(ManifestLine {
                        start_key: "NA".to_string(),
                        end_key: "NA".to_string(),
                        sst_name: line,
                    });
                }
            }
        }
        all_sst_filenames
    }

    fn create_new_level(&mut self) {
        let new_level = self.next_ids.len();
        self.next_ids.push(0);
        let directory_name = format!("{}/l{}", self.data_dir, new_level);
        if !fs::exists(&directory_name).unwrap() {
            fs::create_dir(&directory_name).unwrap();
        }
    }

    pub fn delete(&mut self, key: &str) {
        let entry = serde_json::json!({"op": "delete", "key": key}).to_string();
        let wal_path = format!("{}/wal.db", self.data_dir);
        let mut wal = OpenOptions::new().append(true).open(&wal_path).unwrap();
        writeln!(wal, "{}", entry).unwrap();
        wal.sync_all().unwrap();

        self.memtable.insert(Entry::new(
            key.to_string(),
            Info {
                value: "0".to_string(),
                deleted: true,
            },
        ));
    }
}
