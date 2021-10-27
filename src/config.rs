//! Configuration parsing.

use std::default::Default;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::collections::HashMap;

use log::{debug, warn};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::fmt;
use std::marker::PhantomData;
use structopt::StructOpt;

use crate::models::{Duration,job};

/// Parsed command line options when the server application is started.
#[derive(Debug, StructOpt)]
#[structopt(name = "ocypod")]
pub struct CliOpts {
    #[structopt(parse(from_os_str), help = "Path to configuration file")]
    config: Option<PathBuf>,
}

/// Parses configuration from either configuration path specified in command line arguments,
/// or using default configuration if no configuration file was specified.
pub fn parse_config_from_cli_args() -> Config {
    let opts = CliOpts::from_args();
    let conf = match opts.config {
        Some(config_path) => match Config::from_file(&config_path) {
            Ok(config) => config,
            Err(msg) => {
                eprintln!(
                    "Failed to parse config file {}: {}",
                    &config_path.display(),
                    msg
                );
                std::process::exit(1);
            }
        },
        None => {
            warn!("No config file specified, using default config");
            Config::default()
        }
    };

    // validate config settings
    if let Some(dur) = &conf.server.shutdown_timeout {
        if dur.as_secs() > std::u16::MAX.into() {
            eprintln!("Maximum shutdown_timeout is {} seconds", std::u16::MAX);
            std::process::exit(1);
        }
    }

    conf
}

/// Main application config, typically read from a `.toml` file.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    /// Configuration for the application's HTTP server.
    #[serde(default)]
    pub server: ServerConfig,

    /// Configuration for connecting to Redis.
    #[serde(default)]
    pub redis: RedisConfig,

    /// Option list of queues to be created on application startup.
    pub queue: Option<HashMap<String, crate::models::queue::Settings>>,
}

impl Config {
    /// Read configuration from a file into a new Config struct.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path = path.as_ref();
        debug!("Reading configuration from {}", path.display());

        let data = match fs::read_to_string(path) {
            Ok(data) => data,
            Err(err) => return Err(err.to_string()),
        };

        let conf: Config = match toml::from_str(&data) {
            Ok(conf) => conf,
            Err(err) => return Err(err.to_string()),
        };

        Ok(conf)
    }

    /// Get the address for the HTTP server to listen on.
    pub fn server_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    /// Get the Redis URL to use for connecting to a Redis server.
    pub fn redis_url(&self) -> &str {
        &self.redis.url
    }
}

/// Configuration for the application's HTTP server.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Host address to listen on. Defaults to "127.0.0.1" if not specified.
    pub host: String,

    /// Port to listen on. Defaults to 8023 if not specified.
    pub port: u16,

    /// Number of HTTP worker threads. Defaults to number of CPUs if not specified.
    pub threads: Option<usize>,

    /// Maximum size in bytes for HTTP POST requests. Defaults to "256kB" if not specified.
    #[serde(deserialize_with = "deserialize_human_size")]
    pub max_body_size: Option<usize>,

    /// Determines how often running tasks are checked for timeouts. Defaults to "30s" if not specified.
    pub timeout_check_interval: Duration,

    /// Determines how often failed tasks are checked for retrying. Defaults to "60s" if not specified.
    pub retry_check_interval: Duration,

    /// Determines how often ended tasks are checked for expiry. Defaults to "5m" if not specified.
    pub expiry_check_interval: Duration,

    /// Determines jobs to be expired based on status
    #[serde(deserialize_with = "deserialize_expiry_check_statuses")]
    pub expiry_check_statuses: Vec<job::Status>,

    /// Amount of time workers have to finish requests after server receives SIGTERM.
    pub shutdown_timeout: Option<Duration>,

    /// Adds an artificial delay before returning to clients when a job is requested from an empty queue.
    /// Used to rate limit clients that might be excessively hitting the server, e.g. in tight loops.
    pub next_job_delay: Option<Duration>,

    /// Sets the application-wide log level.
    #[serde(deserialize_with = "deserialize_log_level")]
    pub log_level: log::Level,
}

fn deserialize_human_size<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let s: Option<&str> = Deserialize::deserialize(deserializer)?;
    Ok(match s {
        Some(s) => {
            let size: human_size::SpecificSize<human_size::Byte> = match s.parse() {
                Ok(size) => size,
                Err(_) => {
                    return Err(serde::de::Error::custom(format!(
                        "Unable to parse size '{}'",
                        s
                    )))
                }
            };
            Some(size.value() as usize)
        }
        None => None,
    })
}

fn deserialize_log_level<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<log::Level, D::Error> {
    let s: &str = Deserialize::deserialize(deserializer)?;
    match log::Level::from_str(s) {
        Ok(level) => Ok(level),
        Err(_) => Err(serde::de::Error::custom(format!(
            "Invalid log level: {}",
            s
        ))),
    }
}

fn deserialize_expiry_check_statuses<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<job::Status>, D::Error> {
    struct StringOrVec(PhantomData<Vec<job::Status>>);

    impl<'de> de::Visitor<'de> for StringOrVec {
        type Value = Vec<job::Status>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where E: de::Error
        {
            let status = job::Status::from_str(value.to_owned().as_str()).expect("not a valid expiry_check_status");
            Ok(vec![status])
        }

        fn visit_seq<S>(self, visitor: S) -> Result<Self::Value, S::Error>
            where S: de::SeqAccess<'de>
        {
            Deserialize::deserialize(de::value::SeqAccessDeserializer::new(visitor))
        }
    }

    deserializer.deserialize_any(StringOrVec(PhantomData))
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 8023,
            threads: None,
            max_body_size: None,
            timeout_check_interval: Duration::from_secs(30),
            retry_check_interval: Duration::from_secs(60),
            expiry_check_interval: Duration::from_secs(300),
            expiry_check_statuses: vec![
                job::Status::Failed,
                job::Status::Completed,
                job::Status::Cancelled,
                job::Status::TimedOut
            ],
            shutdown_timeout: None,
            next_job_delay: None,
            log_level: log::Level::Info,
        }
    }
}

/// Configuration for connecting to Redis.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    /// Redis URL to connect to. Defaults to "redis://127.0.0.1".
    pub url: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            url: "redis://127.0.0.1".to_owned(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_minimal() {
        let toml_str = r#"
[server]
host = "0.0.0.0"
port = 8023
log_level = "debug"

[redis]
url = "redis://ocypod-redis"
"#;
        let _: Config = toml::from_str(toml_str).unwrap();
    }

    #[test]
    fn parse_queues() {
        let toml_str = r#"
[server]
host = "::1"
port = 1234
log_level = "info"

[redis]
url = "redis://example.com:6379"

[queue.default]

[queue.another-queue]

[queue.a_3rd_queue]
timeout = "3m"
heartbeat_timeout = "90s"
expires_after = "90m"
retries = 4
retry_delays = ["10s", "1m", "5m"]
"#;
        let conf: Config = toml::from_str(toml_str).unwrap();
        let queues = conf.queue.unwrap();
        assert_eq!(queues.len(), 3);

        assert!(queues.contains_key("default"));
        assert!(queues.contains_key("another-queue"));

        let q3 = &queues["a_3rd_queue"];
        assert_eq!(q3.timeout, Duration::from_secs(180));
        assert_eq!(q3.heartbeat_timeout, Duration::from_secs(90));
        assert_eq!(q3.expires_after, Duration::from_secs(5400));
        assert_eq!(q3.retries, 4);
        assert_eq!(q3.retry_delays, vec![Duration::from_secs(10), Duration::from_secs(60), Duration::from_secs(300)]);
    }
}