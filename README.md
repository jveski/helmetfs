# helmetfs

A FUSE filesystem for real-time backups to remote storage and self-healing from file corruption.

The helmet filesystem is layered on top of two filesystems: one local and one remote.
Operations are passed through to the local fs and asynchronously replicated to the remote one.
A nightly job checks the integrity of local files and recovers from bitflips by restoring from remote storage.


## Installation

```bash
curl -Lo helmetfs "https://github.com/jveski/helmetfs/releases/latest/download/helmetfs-linux-$(uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')"
chmod +x helmetfs
# move it somewhere in your $PATH
```

## Usage

```bash
helmetfs mount <source-dir> <mountpoint> --replica <replica-dir> [options]
helmetfs unmount <mountpoint>
```

### Options

| Flag | Description | Default |
|---|---|---|
| `--replica <path>` | Replica directory (required) | -- |
| `--replication-workers <n>` | Number of background worker threads | `2` |
| `--scrub-interval <seconds>` | Seconds between scrub runs | `86400` |

