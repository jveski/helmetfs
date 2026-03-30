//! CLI entry point: `helmetfs mount` and `helmetfs unmount`.

use helmetfs::{fuse_ops, fuse_sys, replication, scrub, state};

use clap::{Parser, Subcommand};
use std::ffi::CString;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "helmetfs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Mount a FUSE filesystem with replication
    Mount {
        /// Source directory
        source: String,

        /// Mount point
        mountpoint: String,

        /// Replica directory path
        #[arg(long)]
        replica: String,

        /// Number of replication worker threads
        #[arg(long, default_value_t = 2)]
        replication_workers: usize,

        /// Seconds between scrub runs
        #[arg(long, default_value_t = 86400)]
        scrub_interval: u64,
    },

    /// Unmount a FUSE filesystem
    Unmount {
        /// Mount point to unmount
        mountpoint: String,
    },
}

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .format_timestamp_millis()
    .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Mount {
            source,
            mountpoint,
            replica,
            replication_workers,
            scrub_interval,
        } => cmd_mount(source, mountpoint, replica, replication_workers, scrub_interval),
        Command::Unmount { mountpoint } => cmd_unmount(mountpoint),
    }
}

// ---------------------------------------------------------------------------
// mount command
// ---------------------------------------------------------------------------

fn cmd_mount(
    source: String,
    mountpoint: String,
    replica: String,
    repl_workers: usize,
    scrub_interval: u64,
) {
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

fn cmd_unmount(mountpoint: String) {
    let status = process::Command::new("fusermount3")
        .args(["-u", &mountpoint])
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
