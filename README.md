# SwitchNetconf + PortFinder WebApp

A real-time network telemetry pipeline for Cisco IOS-XE devices, with a web tool called **PortFinder** that lets anyone — network engineer or not — look up exactly where a device is connected on the network.

![WebUI](WebUI.png)

## What This Is For

Finding where a device is plugged in is a routine but time-consuming task: cross-reference the MAC address against the MAC address table, find the switch port, look up the VLAN, and match the interface description to figure out which physical switch and closet you're dealing with. Traditionally this means SSH-ing into switches and running several commands. PortFinder eliminates that entirely.

**For network admins**, PortFinder speeds up port location searches, VLAN troubleshooting, and device audits. Instead of jumping between switches, you paste a MAC or IP and immediately see the switch, port, VLAN, and interface description.

**For help desk and non-networking staff**, PortFinder makes it possible to answer "where is this machine connected?" without any knowledge of the network CLI. Enter a MAC address or IP address and get a plain-English result.

**For VLAN troubleshooting**, results show the VLAN number and name alongside the port, so you can quickly identify mismatches — for example, a device that landed on the wrong VLAN, or a port whose native VLAN doesn't match its neighbours.

SwitchNetconf continuously collects MAC address tables, ARP caches, interface configurations, and VLAN data from Cisco IOS-XE devices using Model-Driven Telemetry (MDT) dial-out over gRPC. That data is streamed through a Kafka-compatible broker (Redpanda) and stored in PostgreSQL. PortFinder queries that database in real time.

## How It Works

Cisco IOS-XE devices can be configured to push structured telemetry data to a remote collector using gRPC dial-out. SwitchNetconf receives those streams, decodes the GPB-KV encoded payloads, and writes the data to a Kafka-compatible broker (Redpanda). A second service consumes those messages and upserts the records into PostgreSQL.

```
Cisco IOS-XE Switches / Routers
         │
         │  gRPC dial-out  (TCP 57400)
         │  Cisco MDT – GPB-KV encoding
         ▼
  ┌─────────────┐
  │  Collector  │  Go service – receives gRPC streams,
  │             │  decodes Telemetry protobuf,
  │             │  routes to Kafka topics
  └──────┬──────┘
         │  Kafka topics:
         │    mac-table · arp-table
         │    interface-table · vlan-table
         ▼
  ┌─────────────┐
  │  Redpanda   │  Kafka-compatible broker
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  Consumer   │  Go service – reads topics,
  │             │  upserts rows into PostgreSQL
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │ PostgreSQL  │  Persistent store (SSL required)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐     HTTPS (443)
  │    Nginx    │ ◄──────────────── Browser / API clients
  │  (TLS proxy)│
  └──────┬──────┘
         │  HTTP (internal only)
         ▼
  ┌─────────────┐
  │  PortFinder │  Search by MAC or IP → port, VLAN, node
  │  (webapp)   │
  └─────────────┘

  ┌─────────────┐     HTTPS (8888)          ┌─────────────┐
  │    Nginx    │ ◄──────────────── Browser  │  Kafka UI   │  (disabled by default —
  │  (TLS proxy)│ ───────────────────────►  │             │   enable for troubleshooting)
  └─────────────┘   HTTP (internal only)    └─────────────┘
```

### Telemetry Paths

The collector recognises these IOS-XE YANG paths:

| YANG Path | Kafka Topic | Data |
|-----------|-------------|------|
| `Cisco-IOS-XE-matm-oper:matm-oper-data/matm-table/matm-mac-entry` | `mac-table` | MAC address table |
| `Cisco-IOS-XE-arp-oper:arp-data/arp-vrf/arp-entry` | `arp-table` | ARP cache |
| `arp-ios-xe-oper:arp-data/arp-vrf/arp-entry` | `arp-table` | ARP cache (alt path) |
| `Cisco-IOS-XE-native:native/interface` | `interface-table` | Interface config |
| `openconfig-vlan:vlans/vlan` | `vlan-table` | VLAN table |

Unknown paths are logged and dropped.

---

## Prerequisites

- Docker and Docker Compose
- `openssl` (for certificate generation)
- `psql` or any PostgreSQL client (for running migrations)
  ```
  sudo apt-get install -y postgresql-client-common
  sudo apt-get install -y postgresql-client
  ```

---

## Deployment

### 1. Generate TLS Certificates

Both Nginx (webapp and Kafka UI) and PostgreSQL require TLS certificates. A helper script generates self-signed certificates valid for 10 years:

```bash
bash certs/gen-certs.sh <SERVER_IP>
# e.g. bash certs/gen-certs.sh 192.168.1.50
```

This writes four files:

```
certs/nginx/server.crt       # Nginx TLS certificate
certs/nginx/server.key       # Nginx TLS private key
certs/postgres/server.crt    # PostgreSQL TLS certificate
certs/postgres/server.key    # PostgreSQL TLS private key
certs/postgres/root.crt      # CA cert for client verification
```

To use a certificate signed by your own CA instead, replace the generated files and restart the affected containers:

```bash
# Nginx cert (serves both PortFinder on 443 and Kafka UI on 8888)
cp your-signed.crt certs/nginx/server.crt
cp your-signed.key certs/nginx/server.key
docker compose restart nginx

# PostgreSQL cert
cp your-signed.crt certs/postgres/server.crt
cp your-signed.key certs/postgres/server.key
cp business-ca.crt certs/postgres/root.crt
docker compose restart postgres
```

### 2. Configure Credentials

Copy the example below to `.env` in the project root and set your own values before starting the stack:

```bash
POSTGRES_USER=telemetry
POSTGRES_PASSWORD=telemetry
POSTGRES_DB=telemetry
```

Docker Compose reads this file automatically. The `postgres` container and the `consumer`'s `POSTGRES_DSN` both interpolate these variables at startup.

### 3. Start the Stack

```bash
docker compose up -d
```

This brings up five containers: `redpanda`, `postgres`, `nginx`, `collector`, `webapp`, and `consumer`. Kafka UI is commented out by default — see [Kafka UI](#kafka-ui) below if you need it for troubleshooting. Kafka UI is disabled by default — see [Kafka UI](#kafka-ui) below if you need it.

### 4. Run Database Migrations

Apply the three migration files in order against the running PostgreSQL instance. Source `.env` first so the credentials match what the container was started with:

```bash
source .env
psql "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:15432/${POSTGRES_DB}?sslmode=require" \
  -f migrations/001_init.sql \
  -f migrations/002_interfaces.sql \
  -f migrations/003_vlans.sql
```

The migrations are idempotent (`CREATE TABLE IF NOT EXISTS`), so re-running them is safe.

### 4.5 Restart the Webapp Container

After migrations complete, restart the webapp container so it picks up the newly created tables:

```bash
docker compose restart webapp
```

### 5. Configure Cisco Devices for MDT Dial-Out

On each IOS-XE device, configure a telemetry subscription that dials out to the collector. Replace `<SERVER_IP>` with the IP address of the host running this stack.

```
! Create a destination group pointing at the collector
telemetry ietf subscription 101
 encoding encode-kvgpb
 filter xpath /matm-ios-xe-oper:matm-oper-data/matm-table/matm-mac-entry
 source-address <DEVICE_MGMT_IP>
 stream yang-push
 update-policy periodic 3000
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 102
 encoding encode-kvgpb
 filter xpath /arp-ios-xe-oper:arp-data/arp-vrf/arp-entry
 source-address <DEVICE_MGMT_IP>
 stream yang-push
 update-policy periodic 3000
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 103
 encoding encode-kvgpb
 filter xpath /ios:native/interface
 source-address <DEVICE_MGMT_IP>
 stream yang-push
 update-policy on-change
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 104
 encoding encode-kvgpb
 filter xpath /oc-vlan:vlans/vlan
 source-address <DEVICE_MGMT_IP>
 stream yang-push
 update-policy periodic 6000
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp
```

The `update-policy periodic <ms>` value controls how often the device pushes data. `on-change` only streams when the data changes. Adjust to taste.

Verify the subscriptions are dialling out:

```
show telemetry ietf subscription all
show telemetry internal connection
```

---

## Querying the Data

Connect to PostgreSQL. Source `.env` first so the shell picks up the credentials:

```bash
source .env
psql "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:15432/${POSTGRES_DB}?sslmode=require"
```

For remote connections, pass the CA certificate:

```bash
source .env
psql "postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@<SERVER_IP>:15432/${POSTGRES_DB}?sslmode=verify-ca&sslrootcert=certs/postgres/root.crt"
```

### Example Queries

**Find where a MAC address was last seen:**
```sql
SELECT node_id, interface, vlan, collected_at
FROM mac_table
WHERE mac_address = 'aa:bb:cc:dd:ee:ff';
```

**IP-to-MAC mapping (join ARP and MAC tables):**
```sql
SELECT a.node_id, a.ip_address, a.mac_address, m.interface, m.vlan
FROM arp_table a
JOIN mac_table m ON a.mac_address = m.mac_address;
```

**IP-to-MAC-to-Interface with VLAN info (join ARP, MAC, interface, and VLAN tables)**
```sql
SELECT
    a.node_id,
    a.mac_address,
    m.interface,
    i.description   AS interface_description,
    m.vlan,
    v.name          AS vlan_name
FROM arp_table a
JOIN mac_table       m ON  a.mac_address = m.mac_address
LEFT JOIN interface_table i ON  m.node_id   = i.node_id
                            AND m.interface  = i.name
LEFT JOIN vlan_table      v ON  m.node_id   = v.node_id
                            AND m.vlan       = v.vlan_id
WHERE a.ip_address = '10.0.0.1';
```

**MAC-to-IP-to-Interface with VLAN info (join ARP, MAC, interface, and VLAN tables)**
```sql
SELECT
    m.node_id,
    m.mac_address,
    a.ip_address,
    m.interface,
    i.description   AS interface_description,
    m.vlan,
    v.name          AS vlan_name
FROM mac_table m
LEFT JOIN arp_table       a ON  m.mac_address = a.mac_address
LEFT JOIN interface_table i ON  m.node_id     = i.node_id
                            AND m.interface    = i.name
LEFT JOIN vlan_table      v ON  m.node_id     = v.node_id
                            AND m.vlan         = v.vlan_id
WHERE m.mac_address = 'aa:bb:cc:dd:ee:ff';
```

---

## Database Schema

### `mac_table`
| Column | Type | Description |
|--------|------|-------------|
| `node_id` | text | Device hostname/ID from the telemetry stream |
| `mac_address` | text | MAC address (unique key) |
| `interface` | text | Full interface name (e.g. `GigabitEthernet1/0/1`) |
| `vlan` | integer | VLAN number |
| `mac_type` | text | `dynamic` or `static` |
| `collected_at` | timestamptz | Timestamp from the telemetry message |

### `arp_table`
| Column | Type | Description |
|--------|------|-------------|
| `node_id` | text | Device hostname/ID |
| `ip_address` | text | IP address (unique per node) |
| `mac_address` | text | Resolved MAC address |
| `interface` | text | Interface the ARP entry was learned on |
| `age_seconds` | integer | ARP entry age |
| `collected_at` | timestamptz | Timestamp from the telemetry message |

### `interface_table`
| Column | Type | Description |
|--------|------|-------------|
| `node_id` | text | Device hostname/ID |
| `name` | text | Full interface name (unique per node) |
| `description` | text | Interface description |
| `shutdown` | boolean | `true` if administratively down |
| `ip_address` | text | IP address (L3 interfaces only) |
| `prefix_len` | smallint | Prefix length (e.g. `24`) |
| `vrf` | text | VRF name if configured |
| `mtu` | integer | MTU in bytes |
| `collected_at` | timestamptz | Timestamp from the telemetry message |

### `vlan_table`
| Column | Type | Description |
|--------|------|-------------|
| `node_id` | text | Device hostname/ID |
| `vlan_id` | integer | VLAN number (unique per node) |
| `name` | text | VLAN name |
| `status` | text | `active` or `suspend` |
| `collected_at` | timestamptz | Timestamp from the telemetry message |

All tables use `ON CONFLICT ... DO UPDATE` (upsert), so each row always reflects the most recent telemetry snapshot for that key.

---

## PortFinder

PortFinder is available at `https://<SERVER_IP>` once the stack is running (self-signed certificate — accept the browser warning, or replace with your CA cert).

Enter one or more MAC addresses or IP addresses — one per line — and hit **Search** or **Ctrl+Enter**. All common MAC formats are accepted and normalised automatically:

| Format | Example |
|--------|---------|
| Colon-separated | `aa:bb:cc:dd:ee:ff` |
| Hyphen-separated | `aa-bb-cc-dd-ee-ff` |
| Cisco dot notation | `aabb.ccdd.eeff` |
| Bare hex | `aabbccddeeff` |

All formats are case-insensitive. You can mix MACs and IPs in a single search.

Results always include: the switch hostname (Node ID), MAC address, IP address, switch port (interface), port description, VLAN number, and VLAN name. If a value hasn't been collected yet — for example, a MAC with no ARP entry — that field shows as `—` rather than dropping the row entirely.

An OpenAPI-documented REST API is also available at `/swagger` for integrating PortFinder into scripts, helpdesk tools, or other automation.

---

## Kafka UI

> **Kafka UI is disabled by default.** It is intended for troubleshooting only — enable it temporarily when you need to inspect topics, browse messages, or check consumer group lag, then disable it again.

### Enabling Kafka UI

Make the following four changes, then bring up the services:

**1. Uncomment the `kafka-ui` service block in [docker-compose.yml](docker-compose.yml):**
```yaml
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ...
```

**2. Uncomment `- kafka-ui` in the nginx `depends_on` list in [docker-compose.yml](docker-compose.yml):**
```yaml
    depends_on:
      - kafka-ui   # ← uncomment this line
      - webapp
```

**3. Uncomment the `8888:8888` port in the nginx `ports` list in [docker-compose.yml](docker-compose.yml):**
```yaml
    ports:
      - "8888:8888"   # ← uncomment this line
```

**4. Uncomment the `listen 8888 ssl` server block in [nginx/nginx.conf](nginx/nginx.conf).**

Then start the services:

```bash
docker compose up -d kafka-ui nginx
```

Kafka UI is then available at `https://<SERVER_IP>:8888` (same self-signed certificate as PortFinder — accept the browser warning). Port 8080 is not exposed directly; all access goes through Nginx on port 8888 with TLS.

### Disabling Kafka UI

Re-comment all four items above (reverse steps 1–4), then run:

```bash
docker compose stop kafka-ui && docker compose up -d nginx
```

---

## Environment Variables

### `.env` (Docker Compose credentials)

Docker Compose reads `.env` from the project root automatically.

| Variable | Description |
|----------|-------------|
| `POSTGRES_USER` | PostgreSQL username (used by `postgres` container and consumer DSN) |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |

### Collector
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:19092` | Redpanda/Kafka broker address |
| `GRPC_ADDR` | `:57400` | gRPC listen address for MDT dial-out |

### Consumer
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:19092` | Redpanda/Kafka broker address |
| `POSTGRES_DSN` | *(built from `.env` vars)* | PostgreSQL connection string |

Inside the Docker Compose network the containers use the internal Kafka address (`redpanda:9092`). The external port `19092` is for tooling running on the host.

---

## Security Notes

- PostgreSQL rejects all non-SSL TCP connections from remote hosts (`pg_hba.conf`).
- Credentials are controlled by `.env`. Change `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` there before first start — the `postgres` container and the consumer DSN both pick them up automatically.
- The gRPC listener on port 57400 is unauthenticated — restrict access to trusted network segments via firewall or ACL rules.
- The Nginx certificate is self-signed. Replace it with a certificate from your organisation's CA for production use.
