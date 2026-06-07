package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "embed"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed index.html
var indexHTML string

//go:embed swagger.html
var swaggerHTML string

//go:embed openapi.json
var openapiJSON []byte

var (
	macRe = regexp.MustCompile(`(?i)^([0-9a-f]{2}[:\-]){5}[0-9a-f]{2}$|^[0-9a-f]{4}\.[0-9a-f]{4}\.[0-9a-f]{4}$|^[0-9a-f]{12}$`)
	ipRe  = regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}$`)
)

type Result struct {
	NodeID               string  `json:"node_id"`
	MacAddress           string  `json:"mac_address"`
	IPAddress            *string `json:"ip_address"`
	Interface            *string `json:"interface"`
	InterfaceDescription *string `json:"interface_description"`
	Vlan                 *int32  `json:"vlan"`
	VlanName             *string `json:"vlan_name"`
}

// normalizeMac returns both colon and Cisco-dot forms of a MAC so the query
// matches regardless of which format the device wrote to the database.
func normalizeMac(s string) []string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			b.WriteRune(r)
		}
	}
	if b.Len() != 12 {
		return []string{strings.ToLower(s)}
	}
	h := b.String()
	colon := fmt.Sprintf("%s:%s:%s:%s:%s:%s", h[0:2], h[2:4], h[4:6], h[6:8], h[8:10], h[10:12])
	dot := fmt.Sprintf("%s.%s.%s", h[0:4], h[4:8], h[8:12])
	return []string{colon, dot}
}

// searchSQL finds rows by MAC or IP address.
// The UNION second half catches IPs whose MAC hasn't yet appeared in mac_table.
const searchSQL = `
SELECT
    m.node_id,
    m.mac_address,
    a.ip_address,
    m.interface,
    i.description  AS interface_description,
    m.vlan,
    v.name         AS vlan_name
FROM mac_table m
LEFT JOIN arp_table       a ON  m.mac_address = a.mac_address
LEFT JOIN interface_table i ON  m.node_id     = i.node_id AND m.interface = i.name
LEFT JOIN vlan_table      v ON  m.node_id     = v.node_id AND m.vlan      = v.vlan_id
WHERE lower(m.mac_address) = ANY($1) OR a.ip_address = ANY($2)

UNION

SELECT
    a.node_id,
    COALESCE(a.mac_address, ''),
    a.ip_address,
    NULL::text,
    NULL::text,
    NULL::integer,
    NULL::text
FROM arp_table a
WHERE a.ip_address = ANY($2)
  AND NOT EXISTS (
      SELECT 1 FROM mac_table WHERE lower(mac_address) = lower(a.mac_address)
  )

ORDER BY node_id, mac_address
`

func handleSearch(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Terms []string `json:"terms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		seen := map[string]bool{}
		var macs, ips []string

		for _, t := range req.Terms {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if macRe.MatchString(t) {
				for _, norm := range normalizeMac(t) {
					if !seen[norm] {
						seen[norm] = true
						macs = append(macs, norm)
					}
				}
			} else if ipRe.MatchString(t) {
				if !seen[t] {
					seen[t] = true
					ips = append(ips, t)
				}
			}
		}

		if len(macs) == 0 && len(ips) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]"))
			return
		}

		// pgx serialises nil slices as NULL; empty slices become '{}' which
		// correctly returns no rows from ANY($n) without erroring.
		if macs == nil {
			macs = []string{}
		}
		if ips == nil {
			ips = []string{}
		}

		rows, err := db.Query(context.Background(), searchSQL, macs, ips)
		if err != nil {
			log.Printf("query: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		results := []Result{}
		for rows.Next() {
			var res Result
			if err := rows.Scan(
				&res.NodeID, &res.MacAddress, &res.IPAddress,
				&res.Interface, &res.InterfaceDescription,
				&res.Vlan, &res.VlanName,
			); err != nil {
				log.Printf("scan: %v", err)
				continue
			}
			results = append(results, res)
		}
		if err := rows.Err(); err != nil {
			log.Printf("rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func main() {
	dsn := envOr("POSTGRES_DSN", "postgres://telemetry:telemetry@localhost:15432/telemetry?sslmode=require")

	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, indexHTML)
	})
	mux.HandleFunc("/swagger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, swaggerHTML)
	})
	mux.HandleFunc("/api/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(openapiJSON)
	})
	mux.HandleFunc("/api/search", handleSearch(db))

	addr := envOr("LISTEN_ADDR", ":8888")
	log.Printf("webapp listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
