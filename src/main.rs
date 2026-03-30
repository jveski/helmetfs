//! CLI entry point: `helmetfs mount` and `helmetfs unmount`.
//!
//! Usage:
//!   helmetfs mount <source> <mountpoint> --replica <path> [options]
//!   helmetfs unmount <mountpoint>
//!
//! Options:
//!   --replication-workers <N>   Number of replication worker threads (default: 2)
//!   --scrub-interval <secs>    Seconds between scrub runs (default: 86400)

use helmetfs::{fuse_ops, fuse_sys, replication, scrub, state};

use std::ffi::CString;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .format_timestamp_millis()
    .init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        usage();
        process::exit(1);
    }

    match args[1].as_str() {
        "mount" => cmd_mount(&args[2..]),
        "unmount" => cmd_unmount(&args[2..]),
        "--help" | "-h" | "help" => {
            usage();
            process::exit(0);
        }
        other => {
            eprintln!("Unknown command: {}", other);
            usage();
            process::exit(1);
        }
    }
}

fn usage() {
    eprintln!("Usage:");
    eprintln!("  helmetfs mount <source> <mountpoint> --replica <path> [options]");
    eprintln!("  helmetfs unmount <mountpoint>");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --replication-workers <N>   Worker threads (default: 2)");
    eprintln!("  --scrub-interval <secs>     Scrub interval in seconds (default: 86400)");
}

// ---------------------------------------------------------------------------
// mount command
// ---------------------------------------------------------------------------

fn cmd_mount(args: &[String]) {
    // Parse arguments
    let mut source: Option<String> = None;
    let mut mountpoint: Option<String> = None;
    let mut replica: Option<String> = None;
    let mut repl_workers: usize = 2;
    let mut scrub_interval: u64 = 86400;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--replica" => {
                i += 1;
                replica = Some(args.get(i).cloned().unwrap_or_default());
            }
            "--replication-workers" => {
                i += 1;
                repl_workers = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(2);
            }
            "--scrub-interval" => {
                i += 1;
                scrub_interval = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(86400);
            }
            s if !s.starts_with('-') => {
                if source.is_none() {
                    source = Some(s.to_string());
                } else if mountpoint.is_none() {
                    mountpoint = Some(s.to_string());
                } else {
                    eprintln!("Unexpected argument: {}", s);
                    process::exit(1);
                }
            }
            other => {
                eprintln!("Unknown option: {}", other);
                process::exit(1);
            }
        }
        i += 1;
    }

    let source = source.unwrap_or_else(|| {
        eprintln!("Missing <source> argument");
        process::exit(1);
    });
    let mountpoint = mountpoint.unwrap_or_else(|| {
        eprintln!("Missing <mountpoint> argument");
        process::exit(1);
    });
    let replica = replica.unwrap_or_else(|| {
        eprintln!("Missing --replica argument");
        process::exit(1);
    });

    let source = PathBuf::from(&source)
        .canonicalize()
        .unwrap_or_else(|e| {
            eprintln!("Invalid source path: {}", e);
            process::exit(1);
        });
    let mountpoint_path = PathBuf::from(&mountpoint)
        .canonicalize()
        .unwrap_or_else(|e| {
            eprintln!("Invalid mountpoint path: {}", e);
            process::exit(1);
        });
    let replica = PathBuf::from(&replica)
        .canonicalize()
        .unwrap_or_else(|e| {
            eprintln!("Invalid replica path: {}", e);
            process::exit(1);
        });

    // Ensure replica/files directory exists
    std::fs::create_dir_all(replica.join("files")).unwrap_or_else(|e| {
        eprintln!("Failed to create replica/files directory: {}", e);
        process::exit(1);
    });

    log::info!(
        "Mounting {} on {} with replica {}",
        source.display(),
        mountpoint_path.display(),
        replica.display()
    );

    // Initialize state
    let fs_state = Arc::new(
        state::FsState::new(source, replica, scrub_interval).unwrap_or_else(|e| {
            eprintln!("Failed to initialize state: {}", e);
            process::exit(1);
        }),
    );
    state::set_global_state(fs_state.clone());

    // Start replication workers
    let mut worker_handles = Vec::new();
    for id in 0..repl_workers {
        let st = fs_state.clone();
        let handle = std::thread::Builder::new()
            .name(format!("repl-{}", id))
            .spawn(move || replication::replication_worker(&st))
            .unwrap();
        worker_handles.push(handle);
    }

    // Start scrub thread
    let scrub_state = fs_state.clone();
    let scrub_handle = std::thread::Builder::new()
        .name("scrub".to_string())
        .spawn(move || scrub::scrub_thread(&scrub_state))
        .unwrap();

    // Build FUSE args: program name + mount options
    let prog = CString::new("helmetfs").unwrap();
    let opt_nonempty = CString::new("-odefault_permissions").unwrap();

    let mut c_argv: Vec<*mut libc::c_char> = vec![
        prog.as_ptr() as *mut _,
        opt_nonempty.as_ptr() as *mut _,
    ];

    let mut fuse_args = fuse_sys::fuse_args {
        argc: c_argv.len() as libc::c_int,
        argv: c_argv.as_mut_ptr(),
        allocated: 0,
    };

    // Create FUSE handle
    let fuse = unsafe {
        fuse_sys::fuse_new(
            &mut fuse_args,
            &fuse_ops::FUSE_OPS,
            std::mem::size_of::<fuse_sys::fuse_operations>(),
            std::ptr::null_mut(),
        )
    };
    if fuse.is_null() {
        eprintln!("fuse_new failed");
        process::exit(3);
    }

    // Mount
    let c_mountpoint = CString::new(mountpoint_path.as_os_str().as_encoded_bytes()).unwrap();
    let ret = unsafe { fuse_sys::fuse_mount(fuse, c_mountpoint.as_ptr()) };
    if ret != 0 {
        eprintln!("fuse_mount failed");
        unsafe { fuse_sys::fuse_destroy(fuse) };
        process::exit(4);
    }

    // Set up signal handlers
    let se = unsafe { fuse_sys::fuse_get_session(fuse) };
    if unsafe { fuse_sys::fuse_set_signal_handlers(se) } != 0 {
        eprintln!("Failed to set signal handlers");
        unsafe {
            fuse_sys::fuse_unmount(fuse);
            fuse_sys::fuse_destroy(fuse);
        }
        process::exit(6);
    }

    // Create loop config
    let loop_cfg = unsafe { fuse_sys::fuse_loop_cfg_create() };
    if !loop_cfg.is_null() {
        unsafe {
            fuse_sys::fuse_loop_cfg_set_idle_threads(loop_cfg, 10);
            fuse_sys::fuse_loop_cfg_set_clone_fd(loop_cfg, 0);
        }
    }

    log::info!("FUSE loop starting");

    // Run the multi-threaded FUSE event loop (blocks until unmount/signal)
    let loop_ret = unsafe { fuse_sys::fuse_loop_mt(fuse, loop_cfg) };

    log::info!("FUSE loop exited with code {}", loop_ret);

    // Shutdown: signal workers and scrub to stop
    fs_state.shutting_down.store(true, Ordering::Relaxed);
    fs_state.repl_log.notify_all();

    // Clean up FUSE
    unsafe {
        fuse_sys::fuse_remove_signal_handlers(se);
        fuse_sys::fuse_unmount(fuse);
        fuse_sys::fuse_destroy(fuse);
        if !loop_cfg.is_null() {
            fuse_sys::fuse_loop_cfg_destroy(loop_cfg);
        }
    }

    // Wait for workers
    for handle in worker_handles {
        let _ = handle.join();
    }
    let _ = scrub_handle.join();

    log::info!("helmetfs shutdown complete");
}

// ---------------------------------------------------------------------------
// unmount command
// ---------------------------------------------------------------------------

fn cmd_unmount(args: &[String]) {
    if args.is_empty() {
        eprintln!("Missing <mountpoint> argument");
        process::exit(1);
    }

    let mountpoint = &args[0];
    let status = process::Command::new("fusermount3")
        .args(["-u", mountpoint])
        .status();

    match status {
        Ok(s) if s.success() => {
            log::info!("Unmounted {}", mountpoint);
        }
        Ok(s) => {
            eprintln!("fusermount3 failed with exit code {:?}", s.code());
            process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to run fusermount3: {}", e);
            process::exit(1);
        }
    }
}
