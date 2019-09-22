//! Detect anomalies in the HTTP headers of crate downloads from crates.io.

use lazy_static::lazy_static;
use rayon::prelude::*;
use reqwest::{header::HeaderMap, Response};
use serde::Deserialize;
use walkdir::WalkDir;

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

fn main() {
    let index_path = std::env::args().skip(1).next();
    if index_path.is_none() {
        println!("Usage: {} <path-to-crates.io-index>", std::env::args().next().unwrap());
        return;
    }
    let index_path = index_path.unwrap();
    rayon::ThreadPoolBuilder::new().num_threads(100).build_global().unwrap();
    let client = reqwest::Client::new();
    let versions: Vec<_> = iter_versions(index_path).collect();
    let counter = AtomicU32::new(0);
    versions.par_iter().for_each(|version| {
        version.get_and_check_headers(&client);
        counter.fetch_add(1, Ordering::Relaxed);
    });
    println!("Verified {} versions.", counter.into_inner());
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
struct Version {
    name: String,
    vers: String,
}

impl Version {
    fn get_and_check_headers(&self, client: &reqwest::Client) {
        match self.get_headers(client) {
            Ok(response) => self.check_headers(response.headers()),
            Err(e) => println!("{}: {}", self, e),
        };
    }

    fn get_headers(&self, client: &reqwest::Client) -> reqwest::Result<Response> {
        let url = format!(
            "https://static.crates.io/crates/{crate_name}/{crate_name}-{version}.crate",
            crate_name = self.name,
            version = self.vers,
        );
        client.head(&url).send()
    }

    fn check_headers(&self, headers: &HeaderMap) {
        let actual_keys: HashSet<String> = headers
            .keys()
            .map(|key| key.as_str().to_lowercase())
            .collect();
        lazy_static! {
            static ref EXPECTED_KEYS: HashSet<String> = [
                "content-type",
                "content-length",
                "connection",
                "date",
                "last-modified",
                "etag",
                "x-amz-version-id",
                "accept-ranges",
                "server",
                "x-cache",
                "via",
                "x-amz-cf-pop",
                "x-amz-cf-id",
            ]
            .iter()
            .cloned()
            .map(str::to_owned)
            .collect();
        }
        for key in EXPECTED_KEYS.difference(&actual_keys) {
            println!("{}: Response did not contain '{}' header.", self, key);
        }
        for key in actual_keys.difference(&EXPECTED_KEYS) {
            if key != "age" {
                println!("{}: Response contained unexpected '{}' header.", self, key);
            }
        }
        self.expect_header(headers, "content-type", "application/x-tar");
        self.expect_header(headers, "connection", "keep-alive");
        self.expect_header(headers, "accept-ranges", "bytes");
        self.expect_header(headers, "server", "AmazonS3");
    }

    fn expect_header(&self, headers: &HeaderMap, key: &str, expected_value: &str) {
        if let Some(actual_value) = headers.get(key) {
            if actual_value
                .to_str()
                .map(|s| s != expected_value)
                .unwrap_or(false)
            {
                println!(
                    "{}: Header '{}' has unexpected value '{:?}'.",
                    self, key, actual_value
                );
            }
        }
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name, self.vers)
    }
}

fn iter_versions<P: AsRef<Path>>(index_root: P) -> impl Iterator<Item = Version> {
    use std::io::BufRead;

    WalkDir::new(index_root)
        .into_iter()
        .filter_entry(|entry| {
            let file_name = entry.file_name().to_str().unwrap();
            file_name != ".git" && file_name != "config.json"
        })
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().is_file())
        .flat_map(|entry| {
            let path = entry.into_path();
            let file = std::fs::File::open(&path).unwrap();
            let reader = std::io::BufReader::new(file);
            reader.lines().map(move |line| line.unwrap())
        })
        .map(|line| serde_json::from_str(&line).unwrap())
}
