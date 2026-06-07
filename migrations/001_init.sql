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

CREATE INDEX ON mac_table (mac_address);
CREATE INDEX ON arp_table (ip_address);
CREATE INDEX ON arp_table (mac_address);