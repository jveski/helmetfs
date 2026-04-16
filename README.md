# helmetfs

A FUSE passthrough filesystem with asynchronous replication and self-healing. helmetfs sits transparently between your applications and disk, adding BLAKE3 checksums, background replication to a second directory, and periodic scrubbing to detect and repair silent data corruption.

## Usage

### Mount

```bash
helmetfs mount <source-dir> <mountpoint> --replica <replica-dir> [options]
```

### Unmount

```bash
helmetfs unmount <mountpoint>
```

### Options

| Flag | Description | Default |
|---|---|---|
| `--replica <path>` | Replica directory (required) | -- |
| `--replication-workers <n>` | Number of background worker threads | `4` |
| `--scrub-time HH:MM` | Daily scrub schedule (24-hour format) | `01:00` |
| `--metrics-addr :PORT` | Enable Prometheus metrics endpoint | off |

### Example

```bash
mkdir -p /data/backing /data/replica /mnt/protected

helmetfs mount /data/backing /mnt/protected --replica /data/replica --scrub-time 03:00 --metrics-addr :9090
```

Use `/mnt/protected` as a normal filesystem. Data lives in `/data/backing` and is asynchronously replicated to `/data/replica`.
Internal files (`.sum` sidecars and the `.helmetfs/` directory) are hidden from the mount.

