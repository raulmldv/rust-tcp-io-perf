extern crate bytes;
extern crate rust_tcp_io_perf;
extern crate hdrhist;

use std::time::Instant;
use std::{thread, time};
use rust_tcp_io_perf::config;
use std::convert::TryInto;

use std::os::unix::io::{AsRawFd, RawFd};
use rust_tcp_io_perf::nix::sys::socket::{connect, shutdown, socket, send, recv};
use rust_tcp_io_perf::nix::sys::socket::{AddressFamily, Shutdown, SockAddr, SockFlag, SockType};
use rust_tcp_io_perf::nix::unistd::close;
use rust_tcp_io_perf::nix::errno::Errno::EINTR;
use rust_tcp_io_perf::nix::sys::socket::MsgFlags;
use rust_tcp_io_perf::print_utils;

const MAX_CONNECTION_ATTEMPTS: usize = 5;

struct VsockSocket {
    socket_fd: RawFd,
}

impl VsockSocket {
    fn new(socket_fd: RawFd) -> Self {
        VsockSocket { socket_fd }
    }
}

impl Drop for VsockSocket {
    fn drop(&mut self) {
        shutdown(self.socket_fd, Shutdown::Both)
            .unwrap_or_else(|e| eprintln!("Failed to shut socket down: {:?}", e));
        close(self.socket_fd).unwrap_or_else(|e| eprintln!("Failed to close socket: {:?}", e));
    }
}

impl AsRawFd for VsockSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.socket_fd
    }
}

pub fn send_loop(fd: RawFd, buf: &[u8], len: u64) -> Result<(), String> {
    let len: usize = len.try_into().map_err(|err| format!("{:?}", err))?;
    let mut send_bytes = 0;

    while send_bytes < len {
        let size = match send(fd, &buf[send_bytes..len], MsgFlags::empty()) {
            Ok(size) => size,
            Err(nix::Error::Sys(EINTR)) => 0,
            Err(err) => return Err(format!("{:?}", err)),
        };
        send_bytes += size;
    }

    Ok(())
}

/// Receive `len` bytes from a connection-orriented socket
pub fn recv_loop(fd: RawFd, buf: &mut [u8], len: u64) -> Result<(), String> {
    let len: usize = len.try_into().map_err(|err| format!("{:?}", err))?;
    let mut recv_bytes = 0;

    while recv_bytes < len {
        let size = match recv(fd, &mut buf[recv_bytes..len], MsgFlags::empty()) {
            Ok(size) => size,
            Err(nix::Error::Sys(EINTR)) => 0,
            Err(err) => return Err(format!("{:?}", err)),
        };
        recv_bytes += size;
    }

    Ok(())
}

/// Initiate a connection on an AF_VSOCK socket
fn vsock_connect(cid: u32, port: u32) -> Result<VsockSocket, String> {
    let sockaddr = SockAddr::new_vsock(cid, port);
    let mut err_msg = String::new();

    for i in 0..MAX_CONNECTION_ATTEMPTS {
        let vsocket = VsockSocket::new(
            socket(
                AddressFamily::Vsock,
                SockType::Stream,
                SockFlag::empty(),
                None,
            )
            .map_err(|err| format!("Failed to create the socket: {:?}", err))?,
        );
        match connect(vsocket.as_raw_fd(), &sockaddr) {
            Ok(_) => return Ok(vsocket),
            Err(e) => err_msg = format!("Failed to connect: {}", e),
        }

        // Exponentially backoff before retrying to connect to the socket
        std::thread::sleep(std::time::Duration::from_secs(1 << i));
    }

    Err(err_msg)
}

fn main() {

    let args = config::parse_config();

    println!("Connecting to the server {}...", args.address);
    let n_rounds = args.n_rounds;
    let n_bytes = args.n_bytes;

    // Create buffers to read/write
    let wbuf: Vec<u8> = vec![0; n_bytes];
    let mut rbuf: Vec<u8> = vec![0; n_bytes];

    let progress_tracking_percentage = (n_rounds * 2) / 100;

    let mut connected = false;

    while !connected {
        match vsock_connect(16, 5001) {
            Ok(vsocket) => {
                let fd = vsocket.as_raw_fd();
                connected = true;
                let mut hist = hdrhist::HDRHist::new();

                println!("Connection established! Ready to send...");

                // To avoid TCP slowstart we do double iterations and measure only the second half
                for i in 0..(n_rounds * 2) {

                    let start = Instant::now();

                    send_loop(fd, &wbuf, n_bytes.try_into().unwrap()).expect("Failed send loop.");
                    recv_loop(fd, &mut rbuf, n_bytes.try_into().unwrap()).expect("Failed receive loop.");

                    let duration = Instant::now().duration_since(start);
                    if i >= n_rounds {
                        hist.add_value(duration.as_secs() * 1_000_000_000u64 + duration.subsec_nanos() as u64);
                    }

                    if i % progress_tracking_percentage == 0 {
                        // Track progress on screen
                        println!("{}% completed", i / progress_tracking_percentage);
                    }
                }
                print_utils::print_summary(hist);
            },
            Err(error) => {
                println!("Couldn't connect to server, retrying... Error {}", error);
                thread::sleep(time::Duration::from_secs(1));
            }
        }
    }
}
