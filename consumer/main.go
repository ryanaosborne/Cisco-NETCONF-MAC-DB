package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

// ── Payload structs (mirror collector) ──────────────────────────────────────

type VlanEntry struct {
    Timestamp time.Time `json:"timestamp"`
    NodeID    string    `json:"node_id"`
    VlanID    uint32    `json:"vlan_id"`
    Name      string    `json:"name"`
    Status    string    `json:"status"`
}

type MacEntry struct {
    Timestamp  time.Time `json:"timestamp"`
    NodeID     string    `json:"node_id"`
    MacAddress string    `json:"mac_address"`
    Interface  string    `json:"interface"`
    Vlan       uint32    `json:"vlan"`
    Type       string    `json:"type"`
}

type ArpEntry struct {
    Timestamp  time.Time `json:"timestamp"`
    NodeID     string    `json:"node_id"`
    IPAddress  string    `json:"ip_address"`
    MacAddress string    `json:"mac_address"`
    Interface  string    `json:"interface"`
    Age        uint32    `json:"age"`
}

type IfEntry struct {
    Timestamp   time.Time `json:"timestamp"`
    NodeID      string    `json:"node_id"`
    Name        string    `json:"name"`
    Description *string   `json:"description"`
    Shutdown    *bool     `json:"shutdown"`
    IPAddress   *string   `json:"ip_address"`
    PrefixLen   *int      `json:"prefix_len"`
    VRF         *string   `json:"vrf"`
    MTU         *uint32   `json:"mtu"`
    AccessVlan  *uint16   `json:"access_vlan"`
    VoiceVlan   *uint16   `json:"voice_vlan"`
}

// ── Kafka consumer handler ───────────────────────────────────────────────────

type handler struct{ db *pgxpool.Pool }

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        var err error
        switch msg.Topic {
        case "mac-table":
            err = h.handleMAC(msg.Value)
        case "arp-table":
            err = h.handleARP(msg.Value)
        case "interface-table":
            err = h.handleInterface(msg.Value)
        case "vlan-table":
            err = h.handleVlan(msg.Value)
        }
        if err != nil {
            log.Printf("handle %s: %v", msg.Topic, err)
        } else {
            sess.MarkMessage(msg, "")
        }
    }
    return nil
}

func (h *handler) handleMAC(b []byte) error {
    var e MacEntry
    if err := json.Unmarshal(b, &e); err != nil {
        return err
    }
    _, err := h.db.Exec(context.Background(), `
        INSERT INTO mac_table (node_id, mac_address, interface, vlan, mac_type, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (node_id, mac_address) DO UPDATE SET
            node_id      = EXCLUDED.node_id,
            interface    = EXCLUDED.interface,
            vlan         = EXCLUDED.vlan,
            mac_type     = EXCLUDED.mac_type,
            collected_at = EXCLUDED.collected_at`,
        e.NodeID, e.MacAddress, e.Interface, e.Vlan, e.Type, e.Timestamp)
    return err
}

func (h *handler) handleARP(b []byte) error {
    var e ArpEntry
    if err := json.Unmarshal(b, &e); err != nil {
        return err
    }
    _, err := h.db.Exec(context.Background(), `
        INSERT INTO arp_table (node_id, ip_address, mac_address, interface, age_seconds, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (node_id, ip_address) DO UPDATE SET
            mac_address  = EXCLUDED.mac_address,
            interface    = EXCLUDED.interface,
            age_seconds  = EXCLUDED.age_seconds,
            collected_at = EXCLUDED.collected_at`,
        e.NodeID, e.IPAddress, e.MacAddress, e.Interface, e.Age, e.Timestamp)
    return err
}

func (h *handler) handleInterface(b []byte) error {
    var e IfEntry
    if err := json.Unmarshal(b, &e); err != nil {
        return err
    }
    _, err := h.db.Exec(context.Background(), `
        INSERT INTO interface_table (node_id, name, description, shutdown, ip_address, prefix_len, vrf, mtu, access_vlan, voice_vlan, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9::smallint, 1), $10::smallint, $11)
        ON CONFLICT (node_id, name) DO UPDATE SET
            description  = COALESCE(EXCLUDED.description,         interface_table.description),
            shutdown     = COALESCE(EXCLUDED.shutdown,            interface_table.shutdown),
            ip_address   = COALESCE(EXCLUDED.ip_address,          interface_table.ip_address),
            prefix_len   = COALESCE(EXCLUDED.prefix_len,          interface_table.prefix_len),
            vrf          = COALESCE(EXCLUDED.vrf,                 interface_table.vrf),
            mtu          = COALESCE(EXCLUDED.mtu,                 interface_table.mtu),
            access_vlan  = COALESCE($9::smallint,                 interface_table.access_vlan),
            voice_vlan   = COALESCE($10::smallint,                interface_table.voice_vlan),
            collected_at = EXCLUDED.collected_at`,
        e.NodeID, e.Name, e.Description, e.Shutdown,
        e.IPAddress, e.PrefixLen, e.VRF, e.MTU,
        e.AccessVlan, e.VoiceVlan, e.Timestamp)
    return err
}

func (h *handler) handleVlan(b []byte) error {
    var e VlanEntry
    if err := json.Unmarshal(b, &e); err != nil {
        return err
    }
    _, err := h.db.Exec(context.Background(), `
        INSERT INTO vlan_table (node_id, vlan_id, name, status, collected_at)
        VALUES ($1, $2, NULLIF($3,''), NULLIF($4,''), $5)
        ON CONFLICT (node_id, vlan_id) DO UPDATE SET
            name         = EXCLUDED.name,
            status       = EXCLUDED.status,
            collected_at = EXCLUDED.collected_at`,
        e.NodeID, e.VlanID, e.Name, e.Status, e.Timestamp)
    return err
}

// startCleanup deletes rows older than their configured TTL from all telemetry
// tables, running every minute until ctx is cancelled.
func startCleanup(ctx context.Context, db *pgxpool.Pool, ttls map[string]time.Duration) {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()
    log.Printf("cleanup: started (mac/arp ttl=%s, if/vlan ttl=%s, interval=1m)",
        ttls["mac_table"], ttls["interface_table"])
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            for tbl, ttl := range ttls {
                cutoff := time.Now().Add(-ttl)
                tag, err := db.Exec(ctx, "DELETE FROM "+tbl+" WHERE collected_at < $1", cutoff)
                if err != nil {
                    log.Printf("cleanup %s: %v", tbl, err)
                } else if tag.RowsAffected() > 0 {
                    log.Printf("cleanup %s: removed %d stale rows", tbl, tag.RowsAffected())
                }
            }
        }
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
    broker := envOr("KAFKA_BROKER", "localhost:19092")
    dsn    := envOr("POSTGRES_DSN", "postgres://telemetry:telemetry@localhost:15432/telemetry")

    db, err := pgxpool.New(context.Background(), dsn)
    if err != nil {
        log.Fatalf("postgres: %v", err)
    }
    defer db.Close()

    cfg := sarama.NewConfig()
    cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
    cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
        sarama.NewBalanceStrategyRoundRobin(),
    }

    cg, err := sarama.NewConsumerGroup([]string{broker}, "telemetry-consumer", cfg)
    if err != nil {
        log.Fatalf("consumer group: %v", err)
    }
    defer cg.Close()

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    dynamicTTL, err := time.ParseDuration(envOr("DATA_TTL_DYNAMIC", "5m"))
    if err != nil {
        log.Fatalf("invalid DATA_TTL_DYNAMIC: %v", err)
    }
    staticTTL, err := time.ParseDuration(envOr("DATA_TTL_STATIC", "25h"))
    if err != nil {
        log.Fatalf("invalid DATA_TTL_STATIC: %v", err)
    }
    go startCleanup(ctx, db, map[string]time.Duration{
        "mac_table":       dynamicTTL,
        "arp_table":       dynamicTTL,
        "interface_table": staticTTL,
        "vlan_table":      staticTTL,
    })

    topics := []string{"mac-table", "arp-table", "interface-table", "vlan-table"}
    h := &handler{db: db}

    log.Println("consumer started")
    for {
        if err := cg.Consume(ctx, topics, h); err != nil {
            log.Printf("consume error: %v", err)
        }
        if ctx.Err() != nil {
            return
        }
    }
}

func envOr(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}