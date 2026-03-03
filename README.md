# helmetfs

A FUSE filesystem that bridges between local and remote storage for backups and self-healing.


## Features

TODO


## Usage

```bash
helmetfs mount <source-dir> <mountpoint> --replica <replica-dir> [options]
helmetfs unmount <mountpoint>
```

### Options

| Flag | Description | Default |
|---|---|---|
| `--replica <path>` | Replica directory (required) | -- |
| `--replication-workers <n>` | Number of background worker threads | `4` |
| `--verify-reads` | Enable read-time checksum verification | off |
| `--scrub-time HH:MM` | Daily scrub schedule (24-hour format) | `01:00` |
| `--metrics-addr :PORT` | Enable Prometheus metrics endpoint | off |

