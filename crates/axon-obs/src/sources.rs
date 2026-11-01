use crate::model::{HostSnapshot, ProcessSnapshot};

#[cfg(not(feature = "system-metrics"))]
pub(crate) struct Sources;

#[cfg(not(feature = "system-metrics"))]
impl Sources {
    pub fn new(_process: bool, _host: bool) -> Self {
        Self
    }

    pub fn sample(&mut self) -> (Option<ProcessSnapshot>, Option<HostSnapshot>) {
        (None, None)
    }
}

#[cfg(feature = "system-metrics")]
pub(crate) struct Sources {
    process: bool,
    host: bool,
    warm: bool,
    pid: sysinfo::Pid,
    system: sysinfo::System,
    networks: sysinfo::Networks,
}

#[cfg(feature = "system-metrics")]
impl Sources {
    pub fn new(process: bool, host: bool) -> Self {
        Self {
            process,
            host,
            warm: false,
            pid: sysinfo::Pid::from_u32(std::process::id()),
            system: sysinfo::System::new(),
            networks: sysinfo::Networks::new(),
        }
    }

    /// Only this process is inspected: no process listing, command lines or environment.
    pub fn sample(&mut self) -> (Option<ProcessSnapshot>, Option<HostSnapshot>) {
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate};
        let warm = std::mem::replace(&mut self.warm, true);
        let process = self.process.then(|| {
            let kind = ProcessRefreshKind::nothing().with_cpu().with_memory().with_disk_usage();
            self.system.refresh_processes_specifics(ProcessesToUpdate::Some(&[self.pid]), false, kind);
            self.system.process(self.pid).map(|p| ProcessSnapshot {
                pid: self.pid.as_u32(),
                cpu_percent: warm.then(|| p.cpu_usage()),
                rss_bytes: p.memory(),
                virtual_bytes: p.virtual_memory(),
                read_bytes: p.disk_usage().total_read_bytes,
                written_bytes: p.disk_usage().total_written_bytes,
            })
        });
        let host = self.host.then(|| {
            self.system.refresh_cpu_usage();
            self.system.refresh_memory();
            self.networks.refresh(true);
            #[expect(clippy::disallowed_methods, reason = "the byte counts are sums, the same in any order")]
            let (received_bytes, transmitted_bytes) = (
                self.networks.values().map(|n| n.total_received()).sum(),
                self.networks.values().map(|n| n.total_transmitted()).sum(),
            );
            HostSnapshot {
                cpu_percent: warm.then(|| self.system.global_cpu_usage()),
                cpus: self.system.cpus().len(),
                total_memory_bytes: self.system.total_memory(),
                available_memory_bytes: self.system.available_memory(),
                received_bytes,
                transmitted_bytes,
            }
        });
        (process.flatten(), host)
    }
}
