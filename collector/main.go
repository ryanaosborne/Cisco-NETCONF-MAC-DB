package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
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
	// Name is a pointer so the three telemetry states stay distinct on the wire:
	//   nil  -> field absent (periodic push of a nameless VLAN) -> DB no-op
	//   &""  -> field present and empty (on-change push of a removed name) -> clear
	//   &"x" -> field present with a value -> set
	// omitempty only fires on nil, which is exactly the "absent" case.
	Name   *string `json:"name,omitempty"`
	Status string  `json:"status,omitempty"`
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
	Timestamp time.Time `json:"timestamp"`
	NodeID    string    `json:"node_id"`
	Name      string    `json:"name"`
	// Config-tracked fields are pointers so the three telemetry states stay
	// distinct on the wire, exactly like VlanEntry.Name:
	//   nil    -> field absent (periodic push that omits it)   -> DB no-op
	//   &""/&0 -> field present but empty (on-change removal)   -> clear
	//   &"x"   -> field present with a value                    -> set
	// The collector therefore captures these whenever the field node is present
	// in the message, regardless of value, instead of suppressing empties.
	Description *string `json:"description"`
	Shutdown    bool    `json:"shutdown"`
	IPAddress   *string `json:"ip_address"`
	PrefixLen   *int    `json:"prefix_len"`
	VRF         string  `json:"vrf,omitempty"`
	MTU         uint32  `json:"mtu,omitempty"`
	AccessVlan  *uint16 `json:"access_vlan"`
	VoiceVlan   *uint16 `json:"voice_vlan"`
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
		log.Printf("received msg from %s collection_id=%d path=%s rows=%d", msg.NodeIdStr, msg.CollectionId, msg.EncodingPath, len(msg.DataGpbkv))
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

var intDebug  = envOr("INT_DEBUG",  "false") == "true"
var vlanDebug = envOr("VLAN_DEBUG", "false") == "true"

// showing each field's name, value type, and value (or child fields if it's a
// container). Used to understand the raw structure coming from the switch.
func dumpFields(fields []*telpb.TelemetryField, depth int) string {
	var sb strings.Builder
	pad := strings.Repeat("  ", depth)
	for _, f := range fields {
		var typ, val string
		switch v := f.ValueByType.(type) {
		case *telpb.TelemetryField_StringValue:
			typ, val = "string", fmt.Sprintf("%q", v.StringValue)
		case *telpb.TelemetryField_Uint32Value:
			typ, val = "uint32", fmt.Sprintf("%d", v.Uint32Value)
		case *telpb.TelemetryField_Uint64Value:
			typ, val = "uint64", fmt.Sprintf("%d", v.Uint64Value)
		case *telpb.TelemetryField_Sint32Value:
			typ, val = "sint32", fmt.Sprintf("%d", v.Sint32Value)
		case *telpb.TelemetryField_Sint64Value:
			typ, val = "sint64", fmt.Sprintf("%d", v.Sint64Value)
		case *telpb.TelemetryField_DoubleValue:
			typ, val = "double", fmt.Sprintf("%g", v.DoubleValue)
		case *telpb.TelemetryField_FloatValue:
			typ, val = "float", fmt.Sprintf("%g", v.FloatValue)
		case *telpb.TelemetryField_BoolValue:
			typ, val = "bool", fmt.Sprintf("%t", v.BoolValue)
		case *telpb.TelemetryField_BytesValue:
			typ, val = "bytes", fmt.Sprintf("%x", v.BytesValue)
		default:
			typ = "container"
		}
		if len(f.Fields) > 0 {
			sb.WriteString(fmt.Sprintf("%s[%s] (%s)\n", pad, f.Name, typ))
			sb.WriteString(dumpFields(f.Fields, depth+1))
		} else {
			sb.WriteString(fmt.Sprintf("%s[%s] (%s) = %s\n", pad, f.Name, typ, val))
		}
	}
	return sb.String()
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
	return path == "openconfig-vlan:vlans/vlan" ||
		path == "Cisco-IOS-XE-vlan-oper:vlans/vlan"
}

// expandIfName converts Cisco abbreviated interface names (e.g. "Gi1/0/1",
// "Te1/1/1") to their full forms so they match what the interface parser stores.
func expandIfName(s string) string {
	prefixes := [][2]string{
		{"GigabitEthernet", "Gi"},
		{"FiveGigabitEthernet", "Fi"},
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
	entries := make([]MacEntry, 0, len(t.DataGpbkv))
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
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil
	}
	msg, err := buildMsg("mac-table", t.NodeIdStr, entries)
	if err != nil {
		return err
	}
	_, _, err = s.producer.SendMessage(msg)
	return err
}

// ── ARP table parser ─────────────────────────────────────────────────────────

func (s *telemetryServer) publishARP(t *telpb.Telemetry) error {
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	entries := make([]ArpEntry, 0, len(t.DataGpbkv))
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
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil
	}
	msg, err := buildMsg("arp-table", t.NodeIdStr, entries)
	if err != nil {
		return err
	}
	_, _, err = s.producer.SendMessage(msg)
	return err
}

// ── Interface config parser ──────────────────────────────────────────────────

// findNestedField follows a sequence of field names through the GPB-KV tree.
// Field names are matched after stripping any YANG module prefix
// (e.g. "Cisco-IOS-XE-switch:access" matches the path step "access").
func findNestedField(fields []*telpb.TelemetryField, path ...string) *telpb.TelemetryField {
	cur := fields
	for i, name := range path {
		var match *telpb.TelemetryField
		for _, f := range cur {
			fname := f.Name
			if idx := strings.LastIndex(fname, ":"); idx >= 0 {
				fname = fname[idx+1:]
			}
			if fname == name {
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
	if intDebug {
		log.Printf("INT DEBUG node=%s path=%s collection_id=%d rows=%d",
			t.NodeIdStr, t.EncodingPath, t.CollectionId, len(t.DataGpbkv))
		for i, row := range t.DataGpbkv {
			log.Printf("  row[%d] timestamp=%d\n%s", i, row.Timestamp, dumpFields(row.Fields, 2))
		}
	}
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
					// Capture whenever the field is present: an empty value is a
					// removed description (clear), distinct from the field being
					// absent (no-op). Mirrors the VLAN name handling.
					desc := f.GetStringValue()
					entry.Description = &desc
				case "mtu":
					entry.MTU = f.GetUint32Value()
				case "shutdown":
					entry.Shutdown = true
				case "vrf":
					if fw := findNestedField(f.Fields, "forwarding"); fw != nil {
						entry.VRF = fw.GetStringValue()
					}
				case "ip":
					// Capture address/prefix whenever the field node is present so a
					// removed IP (present, empty) clears the stored value, while an
					// absent ip container (nil) is a no-op.
					if addr := findNestedField(f.Fields, "address", "primary", "address"); addr != nil {
						v := addr.GetStringValue()
						entry.IPAddress = &v
					}
					if mask := findNestedField(f.Fields, "address", "primary", "mask"); mask != nil {
						p := maskToPrefixLen(mask.GetStringValue())
						entry.PrefixLen = &p
					}
				case "switchport-config":
					// Cisco-IOS-XE-switch augments switchport-config/switchport with
					// access/vlan/vlan and voice/vlan/vlan integers. Capture whenever
					// the vlan node is present (even value 0) so a removed assignment
					// clears the stored value; an absent node stays nil (no-op).
					if v := findNestedField(f.Fields, "switchport", "voice", "vlan", "vlan"); v != nil {
						vv := uint16(v.GetUint32Value())
						entry.VoiceVlan = &vv
					}
					if a := findNestedField(f.Fields, "switchport", "access", "vlan", "vlan"); a != nil {
						av := uint16(a.GetUint32Value())
						entry.AccessVlan = &av
					}
				case "switchport":
					// Also check the bare switchport container (duplicate in some
					// IOS-XE versions). Used only as a fallback when switchport-config
					// did not already supply the value.
					if entry.VoiceVlan == nil {
						if v := findNestedField(f.Fields, "voice", "vlan", "vlan"); v != nil {
							vv := uint16(v.GetUint32Value())
							entry.VoiceVlan = &vv
						}
					}
					if entry.AccessVlan == nil {
						if a := findNestedField(f.Fields, "access", "vlan", "vlan"); a != nil {
							av := uint16(a.GetUint32Value())
							entry.AccessVlan = &av
						}
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
	if vlanDebug {
		log.Printf("VLAN DEBUG node=%s path=%s collection_id=%d rows=%d",
			t.NodeIdStr, t.EncodingPath, t.CollectionId, len(t.DataGpbkv))
		for i, row := range t.DataGpbkv {
			log.Printf("  row[%d] timestamp=%d\n%s", i, row.Timestamp, dumpFields(row.Fields, 2))
		}
	}
	ts := time.UnixMilli(int64(t.MsgTimestamp))
	for _, row := range t.DataGpbkv {
		entry := VlanEntry{
			Timestamp: ts,
			NodeID:    t.NodeIdStr,
		}
		for _, container := range row.Fields {
			switch container.Name {
			case "keys":
				// Cisco-IOS-XE-vlan-oper uses "id"; openconfig-vlan uses "vlan-id"
				if f := findNestedField(container.Fields, "id"); f != nil {
					entry.VlanID = f.GetUint32Value()
				} else if f := findNestedField(container.Fields, "vlan-id"); f != nil {
					entry.VlanID = f.GetUint32Value()
				}
			case "content":
				// Cisco-IOS-XE-vlan-oper: name and status are direct children of
				// content. Capture name as a pointer only when the field is actually
				// present, so an absent name (nil) stays distinct from an explicit
				// empty name (&""). An on-change push for a removed VLAN name arrives
				// here as a present, empty-valued field.
				if f := findNestedField(container.Fields, "name"); f != nil {
					v := f.GetStringValue()
					entry.Name = &v
				}
				if f := findNestedField(container.Fields, "status"); f != nil {
					entry.Status = f.GetStringValue()
				}
				// openconfig-vlan: name and status are nested under content → state.
				// Only fall back when the direct "name" field was truly absent, so an
				// explicit empty name is not overwritten by the state lookup.
				if entry.Name == nil {
					if f := findNestedField(container.Fields, "state", "name"); f != nil {
						v := f.GetStringValue()
						entry.Name = &v
					}
				}
				if entry.Status == "" {
					if f := findNestedField(container.Fields, "state", "status"); f != nil {
						entry.Status = f.GetStringValue()
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


// ── Kafka publish helpers ────────────────────────────────────────────────────

func buildMsg(topic, key string, v any) (*sarama.ProducerMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	}, nil
}

func (s *telemetryServer) publish(topic, key string, v any) error {
	msg, err := buildMsg(topic, key, v)
	if err != nil {
		return err
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