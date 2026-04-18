#!/bin/sh
set -e

# Fix ownership of the data volume (mounted at runtime, so Dockerfile chown can't reach it).
# This runs as root before dropping privileges.
mkdir -p /data
chown -R givevc:givevc /data

exec su-exec givevc /run.sh
