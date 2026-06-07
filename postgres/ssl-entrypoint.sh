#!/bin/bash
set -e
# Postgres refuses to start if server.key is group- or world-readable.
# Bind-mounted files are typically owned by root, so we copy them to a
# writable location and fix ownership before handing off to the real entrypoint.
install -m 600 /ssl/server.crt /tmp/pg-server.crt
install -m 600 /ssl/server.key /tmp/pg-server.key
chown postgres:postgres /tmp/pg-server.crt /tmp/pg-server.key
exec /usr/local/bin/docker-entrypoint.sh "$@"
