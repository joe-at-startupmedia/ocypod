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

/// gets file contents
pub fn get_file_contents(filename: &str) -> String {
    let contents = fs::read_to_string(filename).expect(&format!(
        "Something went wrong reading the file: {}",
        &filename
    ));

    contents
}

/// writes job json to a file
pub fn write_job(queue_name: &str, json: &job::CreateRequest) -> Result<(String, i64), Box<dyn std::error::Error>>  {
    
    let paths = get_paths()?;
    
    let output_dir = format!("{}/queues/{}", paths.exe, queue_name);

    fs::create_dir_all(&output_dir)?;
    
    let dt = Utc::now();
    
    let timestamp: i64 = dt.timestamp_millis();

    let file_contents = serde_json::to_string(json)?;

    let destination = format!("{}/{}.json", output_dir, timestamp);

    let _res = fs::write(&destination, file_contents);

    Ok((destination, timestamp))
}

/// get a job creation attempt by the queue_name and timestamp
pub fn get_job(queue_name: &str, timestamp: i64) -> Result<job::CreateRequest, Box<dyn std::error::Error>>  {

    let paths = get_paths()?;
    
    let output_dir = format!("{}/queues/{}", paths.exe, queue_name);

    let src = format!("{}/{}.json", output_dir, timestamp);

    let contents = get_file_contents(&src);

    let create_request = serde_json::from_str(&contents)?;
  
    Ok(create_request)
}

///delete file specified by the path name
pub fn delete_job(location: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::remove_file(location)?;
    Ok(())
}
