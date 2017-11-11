// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use util::config::ReadableSize;
use super::Result;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub dir: String,
    pub bytes_per_sync: ReadableSize,
    pub log_rotate_size: ReadableSize,
    pub total_size_limit: ReadableSize,
    pub recovery_mode: i32,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            bytes_per_sync: ReadableSize::kb(32),
            log_rotate_size: ReadableSize::mb(128),
            total_size_limit: ReadableSize::gb(2),
            recovery_mode: 0,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        if self.total_size_limit.0 <= self.log_rotate_size.0 {
            return Err(box_err!(
                "Total size limit {:?} less than log rotate size {:?}",
                self.total_size_limit,
                self.log_rotate_size
            ));
        }

        if self.recovery_mode < 0 || self.recovery_mode > 1 {
            return Err(box_err!(
                "Unknown recovery mode {} for raftengine",
                self.recovery_mode
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::config::ReadableSize;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::new();
        assert!(cfg.validate().is_ok());

        cfg.recovery_mode = 1;
        assert!(cfg.validate().is_ok());

        cfg.recovery_mode = 2;
        assert!(cfg.validate().is_err());

        cfg.recovery_mode = -1;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.log_rotate_size = ReadableSize::kb(20);
        cfg.total_size_limit = ReadableSize::kb(10);
        assert!(cfg.validate().is_err());

        cfg.total_size_limit = ReadableSize::mb(1);
        assert!(cfg.validate().is_ok());
    }
}
