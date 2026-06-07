CREATE TABLE IF NOT EXISTS vlan_table (
    id           BIGSERIAL PRIMARY KEY,
    node_id      TEXT        NOT NULL,
    vlan_id      INTEGER     NOT NULL,
    name         TEXT,
    status       TEXT,
    collected_at TIMESTAMPTZ NOT NULL,
    UNIQUE (node_id, vlan_id)
);

CREATE INDEX ON vlan_table (vlan_id);
