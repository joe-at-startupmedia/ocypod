//! Handles using the file system as a persistence layer for contingency purposes

use std::path::Path;
use std::{env, fs, str};
use chrono::Utc;

use crate::models::job;

/// stores various paths for writing files to
#[derive(Debug)]
pub struct SysPaths {
    ///the current directory
    pub dir: String,
    ///directory of current executable
    pub exe: String,
}

/// gets paths
pub fn get_paths() -> Result<SysPaths, Box<dyn std::error::Error>> {
    let dir = env::current_dir()?.display().to_string();
    let current_exe = env::current_exe()?.display().to_string();
    let exe = Path::new(&current_exe)
        .parent()
        .unwrap()
        .display()
        .to_string();

    Ok(SysPaths { dir, exe })
}

/// writes job json to a file
pub fn write_job(queue_name: &str, json: &job::CreateRequest) -> Result<String, Box<dyn std::error::Error>>  {
    
    let paths = get_paths()?;
    
    let output_dir = format!("{}/queues/{}", paths.exe, queue_name);

    fs::create_dir_all(&output_dir)?;
    
    let dt = Utc::now();
    
    let timestamp: i64 = dt.timestamp_millis();

    let file_contents = serde_json::to_string(json)?;

    let destination = format!("{}/{}.json", output_dir, timestamp);

    let _res = fs::write(&destination, file_contents);

    Ok(destination)
}

///delete file specified by the path name
pub fn delete_job(location: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::remove_file(location)?;
    Ok(())
}
