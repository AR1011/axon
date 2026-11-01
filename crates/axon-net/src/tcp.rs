//! What the kernel knows about a session's TCP connection.

/// Sampled at each keepalive. `None` fields are ones this platform does not report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TcpHealth {
    /// Smoothed round trip as the kernel measures it from ACKs, below TLS and our framing.
    pub rtt_ns: u64,
    pub rtt_var_ns: u64,
    /// Current retransmission timeout; it grows while the path is losing packets.
    pub rto_ns: u64,
    pub cwnd_bytes: u64,
    pub mss: u32,
    /// Cumulative over the connection.
    pub retransmitted_segments: Option<u64>,
    pub retransmitted_bytes: Option<u64>,
    pub received_out_of_order_bytes: Option<u64>,
    /// Segments currently presumed lost, and sent but not yet acknowledged.
    pub lost_segments: Option<u32>,
    pub unacked_segments: Option<u32>,
    /// Bytes written but still in the kernel send buffer.
    pub send_buffer_bytes: Option<u64>,
    /// `TCP_NOTSENT_LOWAT`: at most this much unsent in the send buffer. `None` where it is the kernel's default or
    /// the platform does not report it.
    pub notsent_lowat: Option<u32>,
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
#[allow(unsafe_code, reason = "getsockopt has no safe wrapper; the SAFETY notes below carry the argument")]
fn info<T>(fd: i32, option: libc::c_int) -> Option<T> {
    let mut info = std::mem::MaybeUninit::<T>::zeroed();
    let mut len = size_of::<T>() as libc::socklen_t;
    // SAFETY: the kernel writes at most `len` bytes into a zeroed plain-integer struct of that size.
    let done = unsafe { libc::getsockopt(fd, libc::IPPROTO_TCP, option, info.as_mut_ptr().cast(), &mut len) };
    // SAFETY: zeroed is a valid value for these all-integer structs, whatever prefix the kernel filled.
    (done == 0).then(|| unsafe { info.assume_init() })
}

/// The caller must own the socket behind `fd` for the duration of the call.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn sample(fd: i32) -> Option<TcpHealth> {
    let i: libc::tcp_info = info(fd, libc::TCP_INFO)?;
    Some(TcpHealth {
        rtt_ns: i.tcpi_rtt as u64 * 1000,
        rtt_var_ns: i.tcpi_rttvar as u64 * 1000,
        rto_ns: i.tcpi_rto as u64 * 1000,
        cwnd_bytes: i.tcpi_snd_cwnd as u64 * i.tcpi_snd_mss as u64,
        mss: i.tcpi_snd_mss,
        retransmitted_segments: Some(i.tcpi_total_retrans as u64),
        lost_segments: Some(i.tcpi_lost),
        unacked_segments: Some(i.tcpi_unacked),
        // 0: not set on the socket, which runs the host's `tcp_notsent_lowat`.
        #[cfg(target_os = "linux")]
        notsent_lowat: info::<libc::c_int>(fd, libc::TCP_NOTSENT_LOWAT)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|&n| n != 0),
        ..Default::default()
    })
}

/// The caller must own the socket behind `fd` for the duration of the call.
#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "setsockopt has no safe wrapper; the SAFETY note below carries the argument")]
pub(crate) fn set_notsent_lowat(fd: i32, bytes: u32) -> std::io::Result<()> {
    let value = libc::c_int::try_from(bytes).unwrap_or(libc::c_int::MAX);
    let len = size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: the kernel reads `len` bytes from a live c_int.
    let done =
        unsafe { libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NOTSENT_LOWAT, (&raw const value).cast(), len) };
    if done == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn set_notsent_lowat(_: i32, _: u32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_vendor = "apple")]
pub(crate) fn sample(fd: i32) -> Option<TcpHealth> {
    let i: libc::tcp_connection_info = info(fd, libc::TCP_CONNECTION_INFO)?;
    Some(TcpHealth {
        rtt_ns: i.tcpi_srtt as u64 * 1_000_000,
        rtt_var_ns: i.tcpi_rttvar as u64 * 1_000_000,
        rto_ns: i.tcpi_rto as u64 * 1_000_000,
        cwnd_bytes: i.tcpi_snd_cwnd as u64,
        mss: i.tcpi_maxseg,
        retransmitted_bytes: Some(i.tcpi_txretransmitbytes),
        received_out_of_order_bytes: Some(i.tcpi_rxoutoforderbytes),
        send_buffer_bytes: Some(i.tcpi_snd_sbbytes as u64),
        ..Default::default()
    })
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn sample(_: i32) -> Option<TcpHealth> {
    None
}

/// This host's TCP limits, from `/proc/sys/net/ipv4`. One connection moves at most `wmem_max` a round trip.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HostTcp {
    /// The largest send buffer the kernel grows a socket to: the third value of `tcp_wmem`.
    pub wmem_max: u64,
    /// The largest receive buffer, the third value of `tcp_rmem`. A receiver advertises about half of it.
    pub rmem_max: u64,
    /// `tcp_congestion_control`, such as `cubic` or `bbr`.
    pub congestion: String,
    /// `tcp_slow_start_after_idle`: an idle connection starts again from a small window.
    pub slow_start_after_idle: bool,
}

/// `None` off Linux, where a file is missing or malformed, and on a thread a simulation seeded: a simulated node has
/// no TCP of its own, and the host's must not reach the simulation's trace.
pub fn host_tcp() -> Option<HostTcp> {
    if axon::trace::simulated() {
        return None;
    }
    #[cfg(target_os = "linux")]
    return parse_host_tcp(std::path::Path::new("/proc/sys/net/ipv4"));
    #[cfg(not(target_os = "linux"))]
    None
}

#[cfg_attr(all(not(target_os = "linux"), not(test)), expect(dead_code, reason = "/proc is Linux's; tested everywhere"))]
fn parse_host_tcp(dir: &std::path::Path) -> Option<HostTcp> {
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).ok();
    let max = |name: &str| -> Option<u64> {
        let values: Vec<u64> = read(name)?.split_whitespace().map(str::parse).collect::<Result<_, _>>().ok()?;
        (values.len() == 3).then(|| values[2])
    };
    let congestion = read("tcp_congestion_control")?.trim().to_owned();
    let slow_start_after_idle = match read("tcp_slow_start_after_idle")?.trim() {
        "0" => false,
        "1" => true,
        _ => return None,
    };
    let (wmem_max, rmem_max) = (max("tcp_wmem")?, max("tcp_rmem")?);
    (!congestion.is_empty()).then_some(HostTcp { wmem_max, rmem_max, congestion, slow_start_after_idle })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_tcp_reads_the_four_files_and_is_none_for_a_missing_or_malformed_one() {
        let dir = std::env::temp_dir().join(format!("axon-host-tcp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let write = |name: &str, body: &str| std::fs::write(dir.join(name), body).unwrap();
        // build-1's values.
        write("tcp_wmem", "4096\t16384\t4194304\n");
        write("tcp_rmem", "4096\t131072\t6291456\n");
        write("tcp_congestion_control", "cubic\n");
        write("tcp_slow_start_after_idle", "1\n");
        let host =
            HostTcp { wmem_max: 4 << 20, rmem_max: 6 << 20, congestion: "cubic".into(), slow_start_after_idle: true };
        assert_eq!(parse_host_tcp(&dir), Some(host));
        write("tcp_wmem", "4096 16384\n");
        assert_eq!(parse_host_tcp(&dir), None, "two values");
        write("tcp_wmem", "4096 16384 lots\n");
        assert_eq!(parse_host_tcp(&dir), None, "not a number");
        write("tcp_wmem", "4096 16384 67108864\n");
        write("tcp_slow_start_after_idle", "2\n");
        assert_eq!(parse_host_tcp(&dir), None, "neither 0 nor 1");
        write("tcp_slow_start_after_idle", "0\n");
        assert!(parse_host_tcp(&dir).is_some_and(|h| h.wmem_max == 64 << 20 && !h.slow_start_after_idle));
        std::fs::remove_file(dir.join("tcp_congestion_control")).unwrap();
        assert_eq!(parse_host_tcp(&dir), None, "a missing file");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn host_tcp_reads_this_linux_host() {
        assert!(host_tcp().is_some_and(|h| h.wmem_max > 0 && h.rmem_max > 0));
    }
}
