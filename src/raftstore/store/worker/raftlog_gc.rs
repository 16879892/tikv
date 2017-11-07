// Copyright 2016 PingCAP, Inc.
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

use util::worker::Runnable;
use raftengine::{MultiRaftEngine as RaftEngine, Result as RaftEngineResult};

use std::sync::Arc;
use std::fmt::{self, Display, Formatter};
use std::error;
use std::sync::mpsc::Sender;

pub enum Task {
    RegionTask(RegionTask),
    EngineTask(EngineTask),
}

impl Task {
    pub fn region_task(
        raft_engine: Arc<RaftEngine>,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) -> Task {
        Task::RegionTask(RegionTask {
            raft_engine: raft_engine,
            region_id: region_id,
            start_idx: start_idx,
            end_idx: end_idx,
        })
    }

    pub fn engine_task(raft_engine: Arc<RaftEngine>) -> Task {
        Task::EngineTask(EngineTask {
            raft_engine: raft_engine,
        })
    }
}

pub struct RegionTask {
    pub raft_engine: Arc<RaftEngine>,
    pub region_id: u64,
    pub start_idx: u64,
    pub end_idx: u64,
}

pub struct EngineTask {
    pub raft_engine: Arc<RaftEngine>,
}

pub struct RegionTaskRes {
    pub collected: u64,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::RegionTask(ref task) => write!(
                f,
                "GC Raft Log Task [region: {}, from: {}, to: {}]",
                task.region_id,
                task.start_idx,
                task.end_idx
            ),
            Task::EngineTask(ref task) => write!(f, "GC raft engine expired files Task"),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("raftlog gc failed {:?}", err)
        }
    }
}

pub struct Runner {
    ch: Option<Sender<RegionTaskRes>>,
}

impl Runner {
    pub fn new(ch: Option<Sender<RegionTaskRes>>) -> Runner {
        Runner { ch: ch }
    }

    /// Do the gc job and return the count of log collected.
    fn gc_raft_log(
        &mut self,
        raft_engine: Arc<RaftEngine>,
        region_id: u64,
        start_idx: u64,
        end_idx: u64,
    ) -> u64 {
        raft_engine.compact_to(region_id, end_idx)
    }

    fn gc_expired_files(&mut self, raft_engine: Arc<RaftEngine>) -> RaftEngineResult<()> {
        if raft_engine.rewrite_inactive() {
            raft_engine.sync_data()?;
        }
        raft_engine.purge_expired_files()
    }

    fn report_collected(&self, collected: u64) {
        if self.ch.is_none() {
            return;
        }
        self.ch
            .as_ref()
            .unwrap()
            .send(RegionTaskRes {
                collected: collected,
            })
            .unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::RegionTask(task) => {
                let n = self.gc_raft_log(
                    task.raft_engine,
                    task.region_id,
                    task.start_idx,
                    task.end_idx,
                );
                debug!("[region {}] collected {} log entries", task.region_id, n);
                self.report_collected(n);
            }
            Task::EngineTask(task) => if let Err(e) = self.gc_expired_files(task.raft_engine) {
                error!("GC expired files for raft engine error : {:?}", e);
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc;
    use std::time::Duration;
    use util::rocksdb::new_engine;
    use tempdir::TempDir;
    use storage::CF_DEFAULT;
    use super::*;

    #[test]
    fn test_gc_raft_log() {
        let path = TempDir::new("gc-raft-log-test").unwrap();
        let raft_db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let raft_db = Arc::new(raft_db);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(Some(tx));

        // generate raft logs
        let region_id = 1;
        let raft_wb = WriteBatch::new();
        for i in 0..100 {
            let k = keys::raft_log_key(region_id, i);
            raft_wb.put(&k, b"entry").unwrap();
        }
        raft_db.write(raft_wb).unwrap();

        let tbls = vec![
            (
                Task {
                    raft_engine: raft_db.clone(),
                    region_id: region_id,
                    start_idx: 0,
                    end_idx: 10,
                },
                10,
                (0, 10),
                (10, 100),
            ),
            (
                Task {
                    raft_engine: raft_db.clone(),
                    region_id: region_id,
                    start_idx: 0,
                    end_idx: 50,
                },
                40,
                (0, 50),
                (50, 100),
            ),
            (
                Task {
                    raft_engine: raft_db.clone(),
                    region_id: region_id,
                    start_idx: 50,
                    end_idx: 50,
                },
                0,
                (0, 50),
                (50, 100),
            ),
            (
                Task {
                    raft_engine: raft_db.clone(),
                    region_id: region_id,
                    start_idx: 50,
                    end_idx: 60,
                },
                10,
                (0, 60),
                (60, 100),
            ),
        ];

        for (task, expected_collectd, not_exist_range, exist_range) in tbls {
            runner.run(task);
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res.collected, expected_collectd);
            raft_log_must_not_exist(&raft_db, 1, not_exist_range.0, not_exist_range.1);
            raft_log_must_exist(&raft_db, 1, exist_range.0, exist_range.1);
        }
    }

    fn raft_log_must_not_exist(raft_engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get(&k).unwrap().is_none());
        }
    }

    fn raft_log_must_exist(raft_engine: &DB, region_id: u64, start_idx: u64, end_idx: u64) {
        for i in start_idx..end_idx {
            let k = keys::raft_log_key(region_id, i);
            assert!(raft_engine.get(&k).unwrap().is_some());
        }
    }
}
