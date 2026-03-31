//! CLI entry point: `helmetfs mount` and `helmetfs unmount`.

use helmetfs::{fuse_ops, replication, scrub, state};

use clap::{Parser, Subcommand};
use fuser::{Config, MountOption};
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

    // Build FUSE filesystem
    let helmet_fs = fuse_ops::HelmetFs::new(fs_state.clone());

    // Mount options
    let mut options = Config::default();
    options.mount_options = vec![
        MountOption::DefaultPermissions,
        MountOption::FSName("helmetfs".to_string()),
        MountOption::AutoUnmount,
    ];

    log::info!("FUSE loop starting");

    // Run the FUSE session (blocks until unmount/signal).
    // fuser::mount2 handles multi-threading, signal handlers, and cleanup
    // internally.
    if let Err(e) = fuser::mount2(helmet_fs, &mountpoint_path, &options) {
        eprintln!("FUSE mount failed: {}", e);
        process::exit(3);
    }

    log::info!("FUSE loop exited");

    // Shutdown: signal workers and scrub to stop
    fs_state.shutting_down.store(true, Ordering::Relaxed);
    fs_state.repl_log.notify_all();

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
