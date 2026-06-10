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
| `Cisco-IOS-XE-vlan-oper:vlans/vlan` | `vlan-table` | VLAN table (IOS-XE native) |
| `openconfig-vlan:vlans/vlan` | `vlan-table` | VLAN table (OpenConfig alt path) |

Unknown paths are logged and dropped.

---

## Prerequisites

- Docker and Docker Compose
- `openssl` (for certificate generation)

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

Copy `example.env` to `.env` in the project root and fill in your values before starting the stack:

```bash
cp example.env .env
# then edit .env with your credentials and settings
```

Docker Compose reads `.env` automatically. The `postgres` container and the `consumer`'s `POSTGRES_DSN` both interpolate these variables at startup. The `.env` file is **not** tracked in git — `example.env` contains the template.

### 2.5. Configure SAML Authentication (optional)

PortFinder supports optional SAML 2.0 authentication via Azure AD (Entra ID). It is disabled by default — set `SAML_ENABLED=true` in `.env` to turn it on.

**Generate an SP signing certificate** (if you don't already have one):

```bash
mkdir -p certs/saml
openssl req -x509 -newkey rsa:2048 \
  -keyout certs/saml/sp.key \
  -out certs/saml/sp.crt \
  -days 3650 -nodes \
  -subj "/CN=portfinder-sp"
```

**Register the app in Azure AD** (Entra ID → Enterprise Applications → New application → Create your own → integrate any other application):

| Azure AD field | Value |
|---|---|
| Identifier (Entity ID) | `https://<SAML_SP_ROOT_URL>/saml/metadata` |
| Reply URL (ACS) | `https://<SAML_SP_ROOT_URL>/saml/acs` |

After saving, copy the **App Federation Metadata URL** from the SAML Certificates section — this is your `SAML_IDP_METADATA_URL` value.

**Set the following in `.env`:**

```bash
SAML_ENABLED=true
SAML_IDP_METADATA_URL=https://login.microsoftonline.com/<TENANT_ID>/federationmetadata/2007-06/federationmetadata.xml?appid=<APP_ID>
SAML_SP_ROOT_URL=https://your-app.example.com
SAML_SP_CERT_FILE=./certs/saml/sp.crt
SAML_SP_KEY_FILE=./certs/saml/sp.key
```

When enabled, all routes (`/`, `/swagger`, `/api/search`) require a valid Azure AD session. The `/saml/` callback routes and `/api/openapi.json` are always accessible without authentication.

### 3. Start the Stack

```bash
docker compose up -d
```

This brings up five containers: `redpanda`, `postgres`, `nginx`, `collector`, `webapp`, and `consumer`. Kafka UI is commented out by default — see [Kafka UI](#kafka-ui) below if you need it for troubleshooting. Kafka UI is disabled by default — see [Kafka UI](#kafka-ui) below if you need it.

### 4. Database Migrations

Database migrations run automatically. `migrations/001_init.sql` is mounted into the PostgreSQL container and executed on first initialisation (when the data volume is empty). No manual migration step is required.

If you ever need to start fresh, destroy the volume first:

```bash
docker compose down -v
docker compose up -d
```

### 5. Configure Cisco Devices for MDT Dial-Out

On each IOS-XE device, configure a telemetry subscription that dials out to the collector. Replace `<SERVER_IP>` with the IP address of the host running this stack.

```
telemetry ietf subscription 101
 encoding encode-kvgpb
 filter xpath /matm-ios-xe-oper:matm-oper-data/matm-table/matm-mac-entry
 stream yang-push
 update-policy periodic 500
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 201
 encoding encode-kvgpb
 filter xpath /arp-ios-xe-oper:arp-data/arp-vrf/arp-entry
 stream yang-push
 update-policy periodic 500
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 301
 encoding encode-kvgpb
 filter xpath /ios:native/interface
 stream yang-push
 update-policy periodic 8640000
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 401
 encoding encode-kvgpb
 filter xpath /ios:native/interface
 stream yang-push
 update-policy on-change
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 501
 encoding encode-kvgpb
 filter xpath /vlan-ios-xe-oper:vlans/vlan
 stream yang-push
 update-policy periodic 1000
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp

telemetry ietf subscription 601
 encoding encode-kvgpb
 filter xpath /vlan-ios-xe-oper:vlans/vlan
 stream yang-push
 update-policy on-change
 receiver ip address <SERVER_IP> 57400 protocol grpc-tcp
```

Interface and VLAN subscriptions use both `periodic` (full state sync) and `on-change` (immediate delta when config changes). MAC and ARP use periodic only. The `periodic <ms>` value controls the push interval — adjust to taste.

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
| `access_vlan` | smallint | Switchport access (data) VLAN; defaults to `1` |
| `voice_vlan` | smallint | Voice VLAN (`NULL` if not configured) |
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

### `.env` (Docker Compose)

Copy `example.env` to `.env` and edit it. Docker Compose reads it automatically from the project root; the file is not tracked in git.

**Database**

| Variable | Description |
|----------|-------------|
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |

**Consumer**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_TTL_DYNAMIC` | `1h` | Retention period for `mac_table` and `arp_table` (Go duration syntax, e.g. `30m`, `2h`). These tables change frequently; set lower to keep the DB lean. |
| `DATA_TTL_STATIC` | `24h` | Retention period for `interface_table` and `vlan_table` (Go duration syntax, e.g. `12h`, `7d`). These change rarely; a longer TTL avoids losing data during brief outages. |

**SAML / Azure AD (webapp)**

| Variable | Default | Description |
|----------|---------|-------------|
| `SAML_ENABLED` | `false` | Set to `true` to require Azure AD login |
| `SAML_IDP_METADATA_URL` | *(none)* | App Federation Metadata URL from Azure AD |
| `SAML_SP_ROOT_URL` | *(none)* | Public base URL of the app, e.g. `https://portfinder.example.com` |
| `SAML_SP_CERT_FILE` | `./certs/saml/sp.crt` | Path to SP signing certificate (PEM) |
| `SAML_SP_KEY_FILE` | `./certs/saml/sp.key` | Path to SP private key (PEM) |

**DB Inspector (webapp)**

| Variable | Default | Description |
|----------|---------|-------------|
| `DBVIEW_ENABLED` | `false` | Set to `true` to expose the raw DB inspector at `/dbview` and `/api/db-inspect`. Intended for troubleshooting only — leave disabled in normal operation. |

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
| `DATA_TTL_DYNAMIC` | `1h` | Retention period for `mac_table` and `arp_table` |
| `DATA_TTL_STATIC` | `24h` | Retention period for `interface_table` and `vlan_table` |

Inside the Docker Compose network the containers use the internal Kafka address (`redpanda:9092`). The external port `19092` is for tooling running on the host.

---

## Security Notes

- PostgreSQL rejects all non-SSL TCP connections from remote hosts (`pg_hba.conf`).
- Credentials are controlled by `.env`. Change `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` there before first start — the `postgres` container and the consumer DSN both pick them up automatically. The `.env` file is excluded from git; never commit it.
- The gRPC listener on port 57400 is unauthenticated — restrict access to trusted network segments via firewall or ACL rules.
- The Nginx certificate is self-signed. Replace it with a certificate from your organisation's CA for production use.
- When `SAML_ENABLED=true`, PortFinder requires a valid Azure AD session for all search and UI routes. The SAML SP certificate in `certs/saml/` is excluded from git and should be kept private.
