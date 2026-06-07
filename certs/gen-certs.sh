#!/usr/bin/env bash
set -euo pipefail

SERVER_IP="${1:?Usage: $0 <SERVER_IP>  (e.g. 192.168.1.50)}"
DAYS=3650

mkdir -p certs/nginx certs/postgres

echo "Generating TLS certificates for IP: $SERVER_IP"

# ── Nginx (Kafka UI) certificate ─────────────────────────────────────────────
openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout certs/nginx/server.key \
  -out    certs/nginx/server.crt \
  -days   "$DAYS" \
  -subj   "/CN=kafka-ui/O=Telemetry" \
  -addext "subjectAltName=IP:$SERVER_IP,IP:127.0.0.1,DNS:localhost"

# ── PostgreSQL certificate ────────────────────────────────────────────────────
openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout certs/postgres/server.key \
  -out    certs/postgres/server.crt \
  -days   "$DAYS" \
  -subj   "/CN=postgres/O=Telemetry" \
  -addext "subjectAltName=IP:$SERVER_IP,IP:127.0.0.1,DNS:localhost,DNS:postgres"

# root.crt is what remote clients use to verify the server cert
cp certs/postgres/server.crt certs/postgres/root.crt

chmod 600 certs/nginx/server.key certs/postgres/server.key

echo ""
echo "Certificates generated. Next steps:"
echo ""
echo "  1. Start services:      docker compose up -d"
echo "  2. Kafka UI (HTTPS):    https://$SERVER_IP"
echo "     Accept the self-signed cert warning in your browser."
echo ""
echo "  3. Remote PostgreSQL:   Copy certs/postgres/root.crt to the remote machine, then:"
echo "     psql \"postgres://telemetry:telemetry@$SERVER_IP:15432/telemetry?sslmode=verify-ca&sslrootcert=/path/to/root.crt\""
echo ""
echo "To replace with your business CA cert:"
echo "  cp your-signed.crt certs/nginx/server.crt  && cp your-signed.key certs/nginx/server.key"
echo "  cp your-signed.crt certs/postgres/server.crt && cp your-signed.key certs/postgres/server.key"
echo "  cp business-ca.crt certs/postgres/root.crt   # CA cert for client verification"
echo "  docker compose restart nginx postgres"
