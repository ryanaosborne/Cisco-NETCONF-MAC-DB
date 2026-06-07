package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	telpb "telemetry/proto"
)

// ── Kafka config ────────────────────────────────────────────────────────────

func newKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.DialTimeout = 10 * time.Second
	return sarama.NewSyncProducer(brokers, cfg)
}

// ── Payload structs (what we publish to Kafka) ──────────────────────────────

type MacEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	NodeID     string    `json:"node_id"`
	MacAddress string    `json:"mac_address"`
	Interface  string    `json:"interface"`
	Vlan       uint32    `json:"vlan"`
	Type       string    `json:"type"` // dynamic / static
}

type VlanEntry struct {
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"node_id"`
	VlanID    uint32    `json:"vlan_id"`
	Name      string    `json:"name,omitempty"`
	Status    string    `json:"status,omitempty"`
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
	Description string    `json:"description"`
	Shutdown    bool      `json:"shutdown"`
	IPAddress   string    `json:"ip_address,omitempty"`
	PrefixLen   int       `json:"prefix_len,omitempty"`
	VRF         string    `json:"vrf,omitempty"`
	MTU         uint32    `json:"mtu,omitempty"`
}

// ── gRPC server ─────────────────────────────────────────────────────────────

type telemetryServer struct {
	telpb.UnimplementedGRPCMdtDialoutServer
	producer sarama.SyncProducer
}

// MdtDialout is the single streaming RPC Cisco calls.
// Each Recv() returns one Telemetry message covering one collection interval.
func (s *telemetryServer) MdtDialout(stream telpb.GRPCMdtDialout_MdtDialoutServer) error {
	log.Printf("MdtDialout stream opened")
	for {
		args, err := stream.Recv()
		if err != nil {
			log.Printf("stream recv error: %v", err)
			return status.Errorf(codes.Unavailable, "stream closed: %v", err)
		}
		if len(args.Data) == 0 {
			continue
		}
		msg := &telpb.Telemetry{}
		if err := proto.Unmarshal(args.Data, msg); err != nil {
			log.Printf("unmarshal telemetry: %v", err)
			continue
		}
		log.Printf("received msg from %s collection_id=%d path=%s", msg.NodeIdStr, msg.CollectionId, msg.EncodingPath)
		if err := s.dispatch(msg); err != nil {
			log.Printf("dispatch error for node %s: %v", msg.NodeIdStr, err)
		}
		if err := stream.Send(&telpb.MdtDialoutResponse{}); err != nil {
			log.Printf("ack send failed (non-fatal): %v", err)
		}
	}
}

// dispatch routes a Telemetry message to the correct topic parser
func (s *telemetryServer) dispatch(t *telpb.Telemetry) error {
	switch {
	case isMAC(t.EncodingPath):
		return s.publishMAC(t)
	case isARP(t.EncodingPath):
		return s.publishARP(t)
	case isInterface(t.EncodingPath):
		return s.publishInterface(t)
	case isVlan(t.EncodingPath):
		return s.publishVlan(t)
	default:
		log.Printf("unhandled path: %s", t.EncodingPath)
		return nil
	}
}

func isMAC(path string) bool {
	// IOS-XE path for MAC address table entries
	return path == "Cisco-IOS-XE-matm-oper:matm-oper-data/matm-table/matm-mac-entry"
}

func isARP(path string) bool {
	return path == "Cisco-IOS-XE-arp-oper:arp-data/arp-vrf/arp-entry" ||
		path == "arp-ios-xe-oper:arp-data/arp-vrf/arp-entry"
}

func isInterface(path string) bool {
	return path == "Cisco-IOS-XE-native:native/interface"
}

func isVlan(path string) bool {
	return path == "openconfig-vlan:vlans/vlan"
}


// expandIfName converts Cisco abbreviated interface names (e.g. "Gi1/0/1",
// "Te1/1/1") to their full forms so they match what the interface parser stores.
func expandIfName(s string) string {
	prefixes := [][2]string{
		{"GigabitEthernet", "Gi"},
		{"TenGigabitEthernet", "Te"},
		{"FastEthernet", "Fa"},
		{"HundredGigE", "Hu"},
		{"TwentyFiveGigE", "Twe"},
		{"FortyGigabitEthernet", "Fo"},
		{"Port-channel", "Po"},
		{"Loopback", "Lo"},
		{"Vlan", "Vl"},
		{"Tunnel", "Tu"},
	}
	for _, p := range prefixes {
		full, abbr := p[0], p[1]
		if len(s) > len(abbr) && s[:len(abbr)] == abbr && s[len(abbr)] >= '0' && s[len(abbr)] <= '9' {
			return full + s[len(abbr):]
		}
	}
	return s
}

// ── MAC table parser ─────────────────────────────────────────────────────────

func (s *telemetryServer) publishMAC(t *telpb.Telemetry) error {
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	for _, row := range t.DataGpbkv {
		entry := MacEntry{
			Timestamp: ts,
			NodeID:    t.NodeIdStr,
		}
		// Cisco GPB-KV nests leaf values one level deep inside "keys"/"content"
		// containers, so we walk both levels.
		applyMAC := func(f *telpb.TelemetryField) {
			switch f.Name {
			case "mac", "mac-address":
				entry.MacAddress = f.GetStringValue()
			case "port", "interface":
				entry.Interface = expandIfName(f.GetStringValue())
			case "vlan-id-number", "vlan-id":
				entry.Vlan = f.GetUint32Value()
			case "mat-addr-type", "mac-type":
				entry.Type = f.GetStringValue()
			}
		}
		for _, container := range row.Fields {
			if len(container.Fields) > 0 {
				for _, f := range container.Fields {
					applyMAC(f)
				}
			} else {
				applyMAC(container)
			}
		}
		if entry.MacAddress == "" {
			continue
		}
		if err := s.publish("mac-table", entry.MacAddress, entry); err != nil {
			return err
		}
	}
	return nil
}

// ── ARP table parser ─────────────────────────────────────────────────────────

func (s *telemetryServer) publishARP(t *telpb.Telemetry) error {
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	for _, row := range t.DataGpbkv {
		entry := ArpEntry{
			Timestamp: ts,
			NodeID:    t.NodeIdStr,
		}
		applyARP := func(f *telpb.TelemetryField) {
			switch f.Name {
			case "address":
				entry.IPAddress = f.GetStringValue()
			case "hardware":
				entry.MacAddress = f.GetStringValue()
			case "interface":
				entry.Interface = expandIfName(f.GetStringValue())
			case "time-to-live":
				entry.Age = f.GetUint32Value()
			}
		}
		for _, container := range row.Fields {
			if len(container.Fields) > 0 {
				for _, f := range container.Fields {
					applyARP(f)
				}
			} else {
				applyARP(container)
			}
		}
		if entry.IPAddress == "" {
			continue
		}
		if err := s.publish("arp-table", entry.IPAddress, entry); err != nil {
			return err
		}
	}
	return nil
}

// ── Interface config parser ──────────────────────────────────────────────────

// findNestedField follows a sequence of field names through the GPB-KV tree.
func findNestedField(fields []*telpb.TelemetryField, path ...string) *telpb.TelemetryField {
	cur := fields
	for i, name := range path {
		var match *telpb.TelemetryField
		for _, f := range cur {
			if f.Name == name {
				match = f
				break
			}
		}
		if match == nil {
			return nil
		}
		if i == len(path)-1 {
			return match
		}
		cur = match.Fields
	}
	return nil
}

// maskToPrefixLen converts a dotted-decimal subnet mask to a prefix length.
func maskToPrefixLen(mask string) int {
	ip := net.ParseIP(mask).To4()
	if ip == nil {
		return 0
	}
	ones, _ := net.IPMask(ip).Size()
	return ones
}

func (s *telemetryServer) publishInterface(t *telpb.Telemetry) error {
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	for _, row := range t.DataGpbkv {
		// The native interface container sends all interface entries inside a
		// single "content" field. Each child of content is named after the
		// interface type (GigabitEthernet, Vlan, Port-channel, etc.) and holds
		// the per-interface config — including the "name" key — as flat siblings.
		var content *telpb.TelemetryField
		for _, f := range row.Fields {
			if f.Name == "content" {
				content = f
				break
			}
		}
		if content == nil {
			continue
		}
		for _, iface := range content.Fields {
			ifType := iface.Name // e.g. "GigabitEthernet", "Vlan", "Port-channel"
			entry := IfEntry{
				Timestamp: ts,
				NodeID:    t.NodeIdStr,
			}
			for _, f := range iface.Fields {
				switch f.Name {
				case "name":
					// GigabitEthernet uses string (e.g. "1/0/1"); Vlan and
					// Port-channel use uint32, so we check both.
					if sv := f.GetStringValue(); sv != "" {
						entry.Name = ifType + sv
					} else {
						entry.Name = fmt.Sprintf("%s%d", ifType, f.GetUint32Value())
					}
				case "description":
					entry.Description = f.GetStringValue()
				case "mtu":
					entry.MTU = f.GetUint32Value()
				case "shutdown":
					entry.Shutdown = true
				case "vrf":
					if fw := findNestedField(f.Fields, "forwarding"); fw != nil {
						entry.VRF = fw.GetStringValue()
					}
				case "ip":
					if addr := findNestedField(f.Fields, "address", "primary", "address"); addr != nil {
						entry.IPAddress = addr.GetStringValue()
					}
					if mask := findNestedField(f.Fields, "address", "primary", "mask"); mask != nil {
						entry.PrefixLen = maskToPrefixLen(mask.GetStringValue())
					}
				}
			}
			if entry.Name == "" {
				continue
			}
			if err := s.publish("interface-table", entry.NodeID+"/"+entry.Name, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// ── VLAN parser ──────────────────────────────────────────────────────────────

func (s *telemetryServer) publishVlan(t *telpb.Telemetry) error {
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	for _, row := range t.DataGpbkv {
		entry := VlanEntry{
			Timestamp: ts,
			NodeID:    t.NodeIdStr,
		}
		for _, container := range row.Fields {
			switch container.Name {
			case "keys":
				if f := findNestedField(container.Fields, "vlan-id"); f != nil {
					entry.VlanID = f.GetUint32Value()
				}
			case "content":
				// state always carries name (synthesized as "VLANxxxx" if not configured)
				// and status; config may omit name, so we prefer state.
				for _, sub := range container.Fields {
					if sub.Name == "state" {
						for _, f := range sub.Fields {
							switch f.Name {
							case "name":
								entry.Name = f.GetStringValue()
							case "status":
								entry.Status = f.GetStringValue()
							}
						}
						break
					}
				}
			}
		}
		if entry.VlanID == 0 {
			continue
		}
		key := fmt.Sprintf("%s/%d", entry.NodeID, entry.VlanID)
		if err := s.publish("vlan-table", key, entry); err != nil {
			return err
		}
	}
	return nil
}


// ── Kafka publish helper ─────────────────────────────────────────────────────

func (s *telemetryServer) publish(topic, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = s.producer.SendMessage(msg)
	return err
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	broker := envOr("KAFKA_BROKER", "localhost:19092")
	grpcAddr := envOr("GRPC_ADDR", ":57400") // 57400 is the Cisco MDT default port

	producer, err := newKafkaProducer([]string{broker})
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	rawLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	lis := &connLogListener{rawLis}

	srv := grpc.NewServer()
	telpb.RegisterGRPCMdtDialoutServer(srv, &telemetryServer{producer: producer})

	log.Printf("collector listening on %s for Cisco MDT streams", grpcAddr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down collector...")
	srv.GracefulStop()
	log.Println("collector stopped")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// connLogListener wraps net.Listener to log source IP and first bytes of every connection.
type connLogListener struct{ net.Listener }

func (l *connLogListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &connLogConn{Conn: conn, maxDump: 2048}, nil
}

type connLogConn struct {
	net.Conn
	mu      sync.Mutex
	total   int
	maxDump int
}

func (c *connLogConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.mu.Lock()
		if c.total < c.maxDump {
			end := c.total + n
			if end > c.maxDump {
				end = c.maxDump
			}
			log.Printf("bytes[%d-%d] from %s: %x", c.total, end, c.RemoteAddr(), b[:end-c.total])
		}
		c.total += n
		c.mu.Unlock()
	}
	return n, err
}
