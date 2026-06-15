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
    // Pointer mirrors the collector: nil = absent (no-op), &"" = clear, &"x" = set.
    // An absent JSON key and an explicit null both unmarshal to nil.
    Name   *string `json:"name"`
    Status string  `json:"status"`
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

const (
    macBatchSize    = 500
    macBatchTimeout = 100 * time.Millisecond
    arpBatchSize    = 500
    arpBatchTimeout = 100 * time.Millisecond
)

type macKey struct{ nodeID, mac string }
type arpKey struct{ nodeID, ip string }

type handler struct{ db *pgxpool.Pool }

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    switch claim.Topic() {
    case "mac-table":
        return h.consumeMACBatch(sess, claim)
    case "arp-table":
        return h.consumeARPBatch(sess, claim)
    }
    for msg := range claim.Messages() {
        var err error
        switch msg.Topic {
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

// consumeMACBatch buffers mac-table messages and flushes them as a single
// batched upsert, either when the batch reaches macBatchSize or macBatchTimeout
// elapses. Entries with the same (node_id, mac_address) key are deduplicated
// within the batch, keeping the one with the latest timestamp.
func (h *handler) consumeMACBatch(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    batch := make(map[macKey]MacEntry, macBatchSize)
    var lastMsg *sarama.ConsumerMessage

    flush := func() {
        if len(batch) == 0 {
            return
        }
        nodeIDs := make([]string, 0, len(batch))
        macs    := make([]string, 0, len(batch))
        ifaces  := make([]string, 0, len(batch))
        vlans   := make([]int32, 0, len(batch))
        types   := make([]string, 0, len(batch))
        times   := make([]time.Time, 0, len(batch))
        for _, e := range batch {
            nodeIDs = append(nodeIDs, e.NodeID)
            macs    = append(macs, e.MacAddress)
            ifaces  = append(ifaces, e.Interface)
            vlans   = append(vlans, int32(e.Vlan))
            types   = append(types, e.Type)
            times   = append(times, e.Timestamp)
        }
        _, err := h.db.Exec(context.Background(), `
            INSERT INTO mac_table (node_id, mac_address, interface, vlan, mac_type, collected_at)
            SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int[], $5::text[], $6::timestamptz[])
                AS t(node_id, mac_address, interface, vlan, mac_type, collected_at)
            ON CONFLICT (node_id, mac_address) DO UPDATE SET
                interface    = EXCLUDED.interface,
                vlan         = EXCLUDED.vlan,
                mac_type     = EXCLUDED.mac_type,
                collected_at = EXCLUDED.collected_at`,
            nodeIDs, macs, ifaces, vlans, types, times)
        if err != nil {
            log.Printf("mac batch flush (%d rows): %v", len(batch), err)
            return
        }
        sess.MarkMessage(lastMsg, "")
        batch = make(map[macKey]MacEntry, macBatchSize)
        lastMsg = nil
    }

    timer := time.NewTimer(macBatchTimeout)
    defer timer.Stop()

    for {
        select {
        case msg, ok := <-claim.Messages():
            if !ok {
                flush()
                return nil
            }
            var entries []MacEntry
            if err := json.Unmarshal(msg.Value, &entries); err != nil {
                log.Printf("handle mac-table: %v", err)
                sess.MarkMessage(msg, "")
                continue
            }
            for _, e := range entries {
                k := macKey{e.NodeID, e.MacAddress}
                if cur, exists := batch[k]; !exists || e.Timestamp.After(cur.Timestamp) {
                    batch[k] = e
                }
            }
            lastMsg = msg
            if len(batch) >= macBatchSize {
                flush()
                if !timer.Stop() {
                    select {
                    case <-timer.C:
                    default:
                    }
                }
                timer.Reset(macBatchTimeout)
            }
        case <-timer.C:
            flush()
            timer.Reset(macBatchTimeout)
        }
    }
}

func (h *handler) consumeARPBatch(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    batch := make(map[arpKey]ArpEntry, arpBatchSize)
    var lastMsg *sarama.ConsumerMessage

    flush := func() {
        if len(batch) == 0 {
            return
        }
        nodeIDs := make([]string, 0, len(batch))
        ips     := make([]string, 0, len(batch))
        macs    := make([]string, 0, len(batch))
        ifaces  := make([]string, 0, len(batch))
        ages    := make([]int32, 0, len(batch))
        times   := make([]time.Time, 0, len(batch))
        for _, e := range batch {
            nodeIDs = append(nodeIDs, e.NodeID)
            ips     = append(ips, e.IPAddress)
            macs    = append(macs, e.MacAddress)
            ifaces  = append(ifaces, e.Interface)
            ages    = append(ages, int32(e.Age))
            times   = append(times, e.Timestamp)
        }
        _, err := h.db.Exec(context.Background(), `
            INSERT INTO arp_table (node_id, ip_address, mac_address, interface, age_seconds, collected_at)
            SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::timestamptz[])
                AS t(node_id, ip_address, mac_address, interface, age_seconds, collected_at)
            ON CONFLICT (node_id, ip_address) DO UPDATE SET
                mac_address  = EXCLUDED.mac_address,
                interface    = EXCLUDED.interface,
                age_seconds  = EXCLUDED.age_seconds,
                collected_at = EXCLUDED.collected_at`,
            nodeIDs, ips, macs, ifaces, ages, times)
        if err != nil {
            log.Printf("arp batch flush (%d rows): %v", len(batch), err)
            return
        }
        sess.MarkMessage(lastMsg, "")
        batch = make(map[arpKey]ArpEntry, arpBatchSize)
        lastMsg = nil
    }

    timer := time.NewTimer(arpBatchTimeout)
    defer timer.Stop()

    for {
        select {
        case msg, ok := <-claim.Messages():
            if !ok {
                flush()
                return nil
            }
            var entries []ArpEntry
            if err := json.Unmarshal(msg.Value, &entries); err != nil {
                log.Printf("handle arp-table: %v", err)
                sess.MarkMessage(msg, "")
                continue
            }
            for _, e := range entries {
                k := arpKey{e.NodeID, e.IPAddress}
                if cur, exists := batch[k]; !exists || e.Timestamp.After(cur.Timestamp) {
                    batch[k] = e
                }
            }
            lastMsg = msg
            if len(batch) >= arpBatchSize {
                flush()
                if !timer.Stop() {
                    select {
                    case <-timer.C:
                    default:
                    }
                }
                timer.Reset(arpBatchTimeout)
            }
        case <-timer.C:
            flush()
            timer.Reset(arpBatchTimeout)
        }
    }
}

func (h *handler) handleInterface(b []byte) error {
    var e IfEntry
    if err := json.Unmarshal(b, &e); err != nil {
        return err
    }
    // description, ip_address, prefix_len, access_vlan and voice_vlan are passed
    // as pointers, so pgx sends NULL for an absent field, the zero value ('' or 0)
    // for an explicit clear, and the real value otherwise. COALESCE(EXCLUDED.x,
    // interface_table.x) then means: NULL leaves the stored value untouched, while
    // a present zero value overwrites it. The WHERE guard on DO UPDATE drops a
    // stale row (e.g. a periodic snapshot that arrives after a newer on-change
    // clear) so it can't resurrect a just-removed value. Mirrors handleVlan.
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
            collected_at = EXCLUDED.collected_at
        WHERE EXCLUDED.collected_at >= interface_table.collected_at`,
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
    // name is passed as a *string ($3), so pgx sends NULL for an absent name,
    // '' for an explicit clear, and the value otherwise. COALESCE(EXCLUDED.name,
    // vlan_table.name) then means: NULL leaves the stored name untouched, while
    // '' overwrites it with an empty string. The WHERE guard on DO UPDATE drops
    // a stale row (e.g. a periodic snapshot that arrives after a newer on-change
    // clear) so it can't resurrect a just-removed name.
    _, err := h.db.Exec(context.Background(), `
        INSERT INTO vlan_table (node_id, vlan_id, name, status, collected_at)
        VALUES ($1, $2, $3, NULLIF($4,''), $5)
        ON CONFLICT (node_id, vlan_id) DO UPDATE SET
            name         = COALESCE(EXCLUDED.name,   vlan_table.name),
            status       = COALESCE(EXCLUDED.status, vlan_table.status),
            collected_at = EXCLUDED.collected_at
        WHERE EXCLUDED.collected_at >= vlan_table.collected_at`,
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