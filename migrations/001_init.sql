CREATE TABLE IF NOT EXISTS mac_table (
    id           BIGSERIAL PRIMARY KEY,
    node_id      TEXT        NOT NULL,
    mac_address  TEXT        NOT NULL,
    interface    TEXT,
    vlan         INTEGER,
    mac_type     TEXT,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (mac_address)
);

CREATE TABLE IF NOT EXISTS arp_table (
    id           BIGSERIAL PRIMARY KEY,
    node_id      TEXT        NOT NULL,
    ip_address   TEXT        NOT NULL,
    mac_address  TEXT,
    interface    TEXT,
    age_seconds  INTEGER,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (node_id, ip_address)
);

CREATE TABLE IF NOT EXISTS interface_table (
    id           BIGSERIAL PRIMARY KEY,
    node_id      TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    description  TEXT,
    shutdown     BOOLEAN     NOT NULL DEFAULT false,
    ip_address   TEXT,
    prefix_len   SMALLINT,
    vrf          TEXT,
    mtu          INTEGER,
    access_vlan  SMALLINT    NOT NULL DEFAULT 1,
    voice_vlan   SMALLINT,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (node_id, name)
);

CREATE TABLE IF NOT EXISTS vlan_table (
    id           BIGSERIAL PRIMARY KEY,
    node_id      TEXT        NOT NULL,
    vlan_id      INTEGER     NOT NULL,
    name         TEXT,
    status       TEXT,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (node_id, vlan_id)
);

CREATE INDEX IF NOT EXISTS mac_table_mac_address   ON mac_table  (mac_address);
CREATE INDEX IF NOT EXISTS arp_table_ip_address    ON arp_table  (ip_address);
CREATE INDEX IF NOT EXISTS arp_table_mac_address   ON arp_table  (mac_address);
CREATE INDEX IF NOT EXISTS interface_table_node    ON interface_table (node_id, name);
CREATE INDEX IF NOT EXISTS interface_table_ip      ON interface_table (ip_address);
CREATE INDEX IF NOT EXISTS interface_table_vlan    ON interface_table (node_id, access_vlan);
CREATE INDEX IF NOT EXISTS vlan_table_vlan_id      ON vlan_table (vlan_id);
