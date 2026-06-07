# Cisco-Stream

A real-time network telemetry pipeline that ingests Model-Driven Telemetry (MDT) streams from Cisco IOS-XE devices and stores them in PostgreSQL for querying.

## How It Works

Cisco IOS-XE devices can be configured to push structured telemetry data to a remote collector using gRPC dial-out. This project receives those streams, decodes the GPB-KV encoded payloads, and writes the data to a Kafka-compatible broker (Redpanda). A second service consumes those messages and upserts the records into PostgreSQL.

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
  └─────────────┘

  ┌─────────────┐     HTTPS (443)
  │    Nginx    │ ◄──────────────── Browser
  │  (TLS proxy)│
  └──────┬──────┘
         │  HTTP (internal only)
         ▼
  ┌─────────────┐
  │  Kafka UI   │  Topic browser / message inspector
  └─────────────┘
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

Both Nginx (Kafka UI) and PostgreSQL require TLS certificates. A helper script generates self-signed certificates valid for 10 years:

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
cp your-signed.crt certs/nginx/server.crt
cp your-signed.key certs/nginx/server.key
cp your-signed.crt certs/postgres/server.crt
cp your-signed.key certs/postgres/server.key
cp business-ca.crt certs/postgres/root.crt
docker compose restart nginx postgres
```

### 2. Start the Stack

```bash
docker compose up -d
```

This brings up six containers: `redpanda`, `postgres`, `kafka-ui`, `nginx`, `collector`, and `consumer`.

### 3. Run Database Migrations

Apply the three migration files in order against the running PostgreSQL instance:

```bash
psql "postgres://telemetry:telemetry@localhost:15432/telemetry?sslmode=require" \
  -f migrations/001_init.sql \
  -f migrations/002_interfaces.sql \
  -f migrations/003_vlans.sql
```

The migrations are idempotent (`CREATE TABLE IF NOT EXISTS`), so re-running them is safe.

### 4. Configure Cisco Devices for MDT Dial-Out

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

Connect to PostgreSQL:

```bash
psql "postgres://telemetry:telemetry@localhost:15432/telemetry?sslmode=require"
```

For remote connections, pass the CA certificate:

```bash
psql "postgres://telemetry:telemetry@<SERVER_IP>:15432/telemetry?sslmode=verify-ca&sslrootcert=certs/postgres/root.crt"
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

## Kafka UI

The Kafka UI is available at `https://<SERVER_IP>` (self-signed certificate — accept the browser warning). It lets you browse topics, inspect messages, and monitor consumer group lag. Port 8080 is not exposed directly; all access goes through Nginx on port 443.

---

## Environment Variables

### Collector
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:19092` | Redpanda/Kafka broker address |
| `GRPC_ADDR` | `:57400` | gRPC listen address for MDT dial-out |

### Consumer
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:19092` | Redpanda/Kafka broker address |
| `POSTGRES_DSN` | `postgres://telemetry:telemetry@localhost:15432/telemetry` | PostgreSQL connection string |

Inside the Docker Compose network the containers use the internal Kafka address (`redpanda:9092`). The external port `19092` is for tooling running on the host.

---

## Security Notes

- PostgreSQL rejects all non-SSL TCP connections from remote hosts (`pg_hba.conf`).
- The default credentials (`telemetry` / `telemetry`) are suitable for a lab environment. Change them for production by updating the `docker-compose.yml` environment variables and the `POSTGRES_DSN` in the consumer.
- The gRPC listener on port 57400 is unauthenticated — restrict access to trusted network segments via firewall or ACL rules.
- The Nginx certificate is self-signed. Replace it with a certificate from your organisation's CA for production use.
