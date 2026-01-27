#!/usr/bin/env bash
set -euo pipefail

# Get the project directory (parent of hack/)
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Create temp directories for development
DATA_DIR=$(mktemp -d)
REMOTE_DIR=$(mktemp -d)

# Resolve symlinks (macOS has /var -> /private/var)
DATA_DIR=$(cd "$DATA_DIR" && pwd -P)
REMOTE_DIR=$(cd "$REMOTE_DIR" && pwd -P)

echo "Data directory: $DATA_DIR"
echo "Remote backup directory: $REMOTE_DIR"

# Write rclone config for local filesystem "remote"
RCLONE_CONFIG="$DATA_DIR/rclone.conf"
cat >"$RCLONE_CONFIG" <<EOF
[devremote]
type = local
EOF

export RCLONE_CONFIG

cleanup() {
	echo "Cleaning up..."
	rm -rf "$DATA_DIR" "$REMOTE_DIR"
}
trap cleanup EXIT

# Build the binary
echo "Building helmetfs..."
BINARY="$DATA_DIR/helmetfs"
go build -o "$BINARY" "$PROJECT_DIR"

# Change to the data directory so helmetfs stores its data there
cd "$DATA_DIR"

# Run helmetfs with the local remote for backups
exec "$BINARY" \
	-debug \
	-rclone-remote "devremote:$REMOTE_DIR" \
	-backup-interval 1m \
	"$@"
