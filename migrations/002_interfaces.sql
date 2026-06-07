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
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (node_id, name)
);

CREATE INDEX ON interface_table (node_id, name);
CREATE INDEX ON interface_table (ip_address);
