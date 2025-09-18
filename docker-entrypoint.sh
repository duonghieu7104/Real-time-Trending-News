#!/bin/bash
set -e

echo ">>> Custom entrypoint, skip chmod/chown data dir"

# Chạy Elasticsearch
exec /usr/local/bin/docker-entrypoint.sh.orig "$@"
