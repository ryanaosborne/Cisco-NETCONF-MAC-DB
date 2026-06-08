package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v3"
)

// ── Device config (from YAML) ────────────────────────────────────────────────

type DevicesFile struct {
	Devices []DeviceConfig `yaml:"devices"`
}

type DeviceConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	NodeID   string `yaml:"node_id"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	TLS      bool   `yaml:"tls"`
	CACert   string `yaml:"ca_cert,omitempty"`
}

func loadDevices(path string) ([]DeviceConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var df DevicesFile
	if err := yaml.NewDecoder(f).Decode(&df); err != nil {
		return nil, err
	}
	return df.Devices, nil
}

// ── Kafka ────────────────────────────────────────────────────────────────────

func newKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.DialTimeout = 10 * time.Second
	return sarama.NewSyncProducer(brokers, cfg)
}

// ── Kafka payload structs (consumer expects these exact shapes) ───────────────

type MacEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	NodeID     string    `json:"node_id"`
	MacAddress string    `json:"mac_address"`
	Interface  string    `json:"interface"`
	Vlan       uint32    `json:"vlan"`
	Type       string    `json:"type"`
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

// ── JSON_IETF unmarshal types ─────────────────────────────────────────────────

type jsonMacEntry struct {
	VlanID    uint32 `json:"vlan-id-number"`
	Mac       string `json:"mac-address"`
	Interface string `json:"interface"`
	Type      string `json:"mat-addr-type"`
}

type jsonArpEntry struct {
	Address   string `json:"address"`
	Hardware  string `json:"hardware"`
	Interface string `json:"interface"`
	TTL       uint32 `json:"time-to-live"`
}

type jsonVlanState struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type jsonVlan struct {
	VlanID uint32        `json:"vlan-id"`
	State  jsonVlanState `json:"state"`
}

type jsonIfEntry struct {
	Name        json.RawMessage  `json:"name"`
	Description string           `json:"description"`
	Shutdown    *json.RawMessage `json:"shutdown"` // presence container: non-nil = shutdown
	MTU         uint32           `json:"mtu"`
	VRF         *struct {
		Forwarding string `json:"forwarding"`
	} `json:"vrf"`
	IP *struct {
		Address *struct {
			Primary *struct {
				Address string `json:"address"`
				Mask    string `json:"mask"`
			} `json:"primary"`
		} `json:"address"`
	} `json:"ip"`
}

// ── gNMI subscriptions ────────────────────────────────────────────────────────

type subSpec struct {
	path     string
	mode     gnmipb.SubscriptionMode
	interval time.Duration // only meaningful for SAMPLE
}

var subscriptions = []subSpec{
	{
		path:     "Cisco-IOS-XE-matm-oper:matm-oper-data/matm-table/matm-mac-entry",
		mode:     gnmipb.SubscriptionMode_SAMPLE,
		interval: 60 * time.Second,
	},
	{
		path:     "Cisco-IOS-XE-arp-oper:arp-data/arp-vrf/arp-entry",
		mode:     gnmipb.SubscriptionMode_SAMPLE,
		interval: 60 * time.Second,
	},
	{
		path:  "Cisco-IOS-XE-native:native/interface",
		mode:  gnmipb.SubscriptionMode_ON_CHANGE,
	},
	{
		path:     "openconfig-vlan:vlans/vlan",
		mode:     gnmipb.SubscriptionMode_SAMPLE,
		interval: 60 * time.Second,
	},
}

// parsePath converts "module:path/to/leaf" into a *gnmipb.Path with origin set.
func parsePath(s string) *gnmipb.Path {
	origin, rest := "", s
	if idx := strings.Index(s, ":"); idx != -1 {
		origin = s[:idx]
		rest = s[idx+1:]
	}
	var elems []*gnmipb.PathElem
	for _, part := range strings.Split(strings.Trim(rest, "/"), "/") {
		if part != "" {
			elems = append(elems, &gnmipb.PathElem{Name: part})
		}
	}
	return &gnmipb.Path{Origin: origin, Elem: elems}
}

// ── Subscriber ────────────────────────────────────────────────────────────────

type subscriber struct {
	dev      DeviceConfig
	producer sarama.SyncProducer
}

func (s *subscriber) nodeID() string {
	if s.dev.NodeID != "" {
		return s.dev.NodeID
	}
	return s.dev.Host
}

func (s *subscriber) dialOpts() ([]grpc.DialOption, error) {
	if !s.dev.TLS {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, nil
	}
	tlsCfg := &tls.Config{}
	if s.dev.CACert != "" {
		pem, err := os.ReadFile(s.dev.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca_cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_cert: no valid PEM certificates found")
		}
		tlsCfg.RootCAs = pool
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}, nil
}

// run keeps the subscription alive with reconnect backoff until ctx is cancelled.
func (s *subscriber) run(ctx context.Context) {
	nid := s.nodeID()
	for {
		if err := s.connect(ctx, nid); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] disconnected: %v — retrying in 30s", nid, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

func (s *subscriber) connect(ctx context.Context, nid string) error {
	opts, err := s.dialOpts()
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("%s:%d", s.dev.Host, s.dev.Port)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	log.Printf("[%s] connected to %s", nid, addr)

	ctx = metadata.AppendToOutgoingContext(ctx,
		"username", s.dev.Username,
		"password", s.dev.Password,
	)

	client := gnmipb.NewGNMIClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("Subscribe: %w", err)
	}

	subs := make([]*gnmipb.Subscription, 0, len(subscriptions))
	for _, spec := range subscriptions {
		sub := &gnmipb.Subscription{
			Path: parsePath(spec.path),
			Mode: spec.mode,
		}
		if spec.mode == gnmipb.SubscriptionMode_SAMPLE {
			sub.SampleInterval = uint64(spec.interval)
		}
		subs = append(subs, sub)
	}

	if err := stream.Send(&gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Subscribe{
			Subscribe: &gnmipb.SubscriptionList{
				Mode:         gnmipb.SubscriptionList_STREAM,
				Encoding:     gnmipb.Encoding_JSON_IETF,
				Subscription: subs,
			},
		},
	}); err != nil {
		return fmt.Errorf("send SubscribeRequest: %w", err)
	}

	log.Printf("[%s] subscribed to %d paths", nid, len(subscriptions))

	for {
		resp, err := stream.Recv()
		if err == io.EOF || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("Recv: %w", err)
		}
		switch r := resp.Response.(type) {
		case *gnmipb.SubscribeResponse_Update:
			notif := r.Update
			ts := time.Unix(0, notif.Timestamp)
			for _, upd := range notif.Update {
				if err := s.dispatch(upd, nid, ts); err != nil {
					log.Printf("[%s] dispatch: %v", nid, err)
				}
			}
		case *gnmipb.SubscribeResponse_SyncResponse:
			log.Printf("[%s] initial sync complete", nid)
		}
	}
}

// dispatch routes an Update to the correct parser by path origin and tail element.
func (s *subscriber) dispatch(upd *gnmipb.Update, nid string, ts time.Time) error {
	val := upd.Val.GetJsonIetfVal()
	if len(val) == 0 {
		return nil
	}
	var origin, tail string
	if p := upd.Path; p != nil {
		origin = p.Origin
		if len(p.Elem) > 0 {
			tail = p.Elem[len(p.Elem)-1].Name
		}
	}
	switch {
	case origin == "Cisco-IOS-XE-matm-oper" || tail == "matm-mac-entry":
		return s.publishMAC(val, nid, ts)
	case origin == "Cisco-IOS-XE-arp-oper" || tail == "arp-entry":
		return s.publishARP(val, nid, ts)
	case origin == "Cisco-IOS-XE-native" || tail == "interface":
		return s.publishInterface(val, nid, ts)
	case origin == "openconfig-vlan" || tail == "vlan":
		return s.publishVlan(val, nid, ts)
	default:
		log.Printf("[%s] unhandled path origin=%q tail=%q", nid, origin, tail)
		return nil
	}
}

// unwrapList decodes a JSON payload as a slice of entries. Cisco IOS-XE gNMI
// responses are either a bare JSON array or a single-key wrapper object
// (namespace-prefixed key pointing to the array), so we handle both.
func unwrapList(payload []byte) []json.RawMessage {
	var arr []json.RawMessage
	if json.Unmarshal(payload, &arr) == nil {
		return arr
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(payload, &obj) == nil {
		for _, v := range obj {
			var inner []json.RawMessage
			if json.Unmarshal(v, &inner) == nil {
				return inner
			}
			return []json.RawMessage{v}
		}
	}
	return []json.RawMessage{payload}
}

// ── MAC ───────────────────────────────────────────────────────────────────────

func (s *subscriber) publishMAC(payload []byte, nid string, ts time.Time) error {
	for _, raw := range unwrapList(payload) {
		var e jsonMacEntry
		if json.Unmarshal(raw, &e) != nil || e.Mac == "" {
			continue
		}
		if err := s.publish("mac-table", e.Mac, MacEntry{
			Timestamp:  ts,
			NodeID:     nid,
			MacAddress: e.Mac,
			Interface:  expandIfName(e.Interface),
			Vlan:       e.VlanID,
			Type:       e.Type,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ── ARP ───────────────────────────────────────────────────────────────────────

func (s *subscriber) publishARP(payload []byte, nid string, ts time.Time) error {
	for _, raw := range unwrapList(payload) {
		var e jsonArpEntry
		if json.Unmarshal(raw, &e) != nil || e.Address == "" {
			continue
		}
		if err := s.publish("arp-table", e.Address, ArpEntry{
			Timestamp:  ts,
			NodeID:     nid,
			IPAddress:  e.Address,
			MacAddress: e.Hardware,
			Interface:  expandIfName(e.Interface),
			Age:        e.TTL,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ── Interface ─────────────────────────────────────────────────────────────────

func (s *subscriber) publishInterface(payload []byte, nid string, ts time.Time) error {
	// Top-level JSON is { "ifType": [...], ... } or wrapped as
	// { "Cisco-IOS-XE-native:interface": { "ifType": [...], ... } }.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		return fmt.Errorf("interface: %w", err)
	}
	// Unwrap single-key namespace wrapper if present.
	if len(top) == 1 {
		for _, v := range top {
			var inner map[string]json.RawMessage
			if json.Unmarshal(v, &inner) == nil {
				top = inner
			}
		}
	}
	for ifType, listRaw := range top {
		if idx := strings.Index(ifType, ":"); idx != -1 {
			ifType = ifType[idx+1:]
		}
		for _, raw := range unwrapList(listRaw) {
			var e jsonIfEntry
			if json.Unmarshal(raw, &e) != nil {
				continue
			}
			name := parseIfName(ifType, e.Name)
			if name == "" {
				continue
			}
			entry := IfEntry{
				Timestamp:   ts,
				NodeID:      nid,
				Name:        name,
				Description: e.Description,
				Shutdown:    e.Shutdown != nil,
				MTU:         e.MTU,
			}
			if e.VRF != nil {
				entry.VRF = e.VRF.Forwarding
			}
			if e.IP != nil && e.IP.Address != nil && e.IP.Address.Primary != nil {
				entry.IPAddress = e.IP.Address.Primary.Address
				entry.PrefixLen = maskToPrefixLen(e.IP.Address.Primary.Mask)
			}
			if err := s.publish("interface-table", nid+"/"+name, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// parseIfName converts ifType + raw JSON name (string or number) into a full
// interface name. Vlan and Port-channel use integer names; physical ports use strings.
func parseIfName(ifType string, raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return ifType + s
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return ifType + n.String()
	}
	return ""
}

// ── VLAN ──────────────────────────────────────────────────────────────────────

func (s *subscriber) publishVlan(payload []byte, nid string, ts time.Time) error {
	for _, raw := range unwrapList(payload) {
		var e jsonVlan
		if json.Unmarshal(raw, &e) != nil || e.VlanID == 0 {
			continue
		}
		if err := s.publish("vlan-table", fmt.Sprintf("%s/%d", nid, e.VlanID), VlanEntry{
			Timestamp: ts,
			NodeID:    nid,
			VlanID:    e.VlanID,
			Name:      e.State.Name,
			Status:    e.State.Status,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func expandIfName(s string) string {
	for _, p := range [][2]string{
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
	} {
		full, abbr := p[0], p[1]
		if len(s) > len(abbr) && s[:len(abbr)] == abbr && s[len(abbr)] >= '0' && s[len(abbr)] <= '9' {
			return full + s[len(abbr):]
		}
	}
	return s
}

func maskToPrefixLen(mask string) int {
	ip := net.ParseIP(mask).To4()
	if ip == nil {
		return 0
	}
	ones, _ := net.IPMask(ip).Size()
	return ones
}

func (s *subscriber) publish(topic, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	})
	return err
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	broker := envOr("KAFKA_BROKER", "localhost:19092")
	devicesFile := envOr("DEVICES_FILE", "devices.yaml")

	devices, err := loadDevices(devicesFile)
	if err != nil {
		log.Fatalf("load devices: %v", err)
	}
	if len(devices) == 0 {
		log.Fatalf("no devices configured in %s", devicesFile)
	}

	producer, err := newKafkaProducer([]string{broker})
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for _, dev := range devices {
		wg.Add(1)
		go func(d DeviceConfig) {
			defer wg.Done()
			(&subscriber{dev: d, producer: producer}).run(ctx)
		}(dev)
	}

	<-ctx.Done()
	log.Println("shutting down collector...")
	wg.Wait()
	log.Println("collector stopped")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
