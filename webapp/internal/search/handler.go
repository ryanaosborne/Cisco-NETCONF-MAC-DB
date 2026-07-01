package search

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Handler(db *pgxpool.Pool) http.HandlerFunc {
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
		var macs, ips, hostnames []string
		var termRecords []termRecord

		for _, t := range req.Terms {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if MacRe.MatchString(t) {
				norms := normalizeMac(t)
				for _, norm := range norms {
					if !seen[norm] {
						seen[norm] = true
						macs = append(macs, norm)
					}
				}
				termRecords = append(termRecords, termRecord{original: t, kind: "mac", lookFor: norms})
			} else if IPRe.MatchString(t) {
				if !seen[t] {
					seen[t] = true
					ips = append(ips, t)
				}
				termRecords = append(termRecords, termRecord{original: t, kind: "ip", lookFor: []string{t}})
			} else if HostRe.MatchString(t) {
				if !seen[t] {
					seen[t] = true
					hostnames = append(hostnames, t)
				}
				termRecords = append(termRecords, termRecord{original: t, kind: "hostname"})
			}
		}

		hostnameIPs := map[string][]string{}
		for _, hostname := range hostnames {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			addrs, err := net.DefaultResolver.LookupHost(ctx, hostname)
			cancel()
			if err != nil {
				log.Printf("search: lookup %q: %v", hostname, err)
				continue
			}
			var resolved []string
			for _, addr := range addrs {
				if IPRe.MatchString(addr) {
					if !seen[addr] {
						seen[addr] = true
						ips = append(ips, addr)
					}
					resolved = append(resolved, addr)
				}
			}
			hostnameIPs[hostname] = resolved
		}
		for i, tr := range termRecords {
			if tr.kind == "hostname" {
				termRecords[i].lookFor = hostnameIPs[tr.original]
			}
		}

		writeResp := func(resp SearchResponse) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}

		if len(macs) == 0 && len(ips) == 0 {
			notFound := make([]string, 0, len(termRecords))
			for _, tr := range termRecords {
				notFound = append(notFound, tr.original)
			}
			writeResp(SearchResponse{Results: []Result{}, NotFound: notFound})
			return
		}

		if macs == nil {
			macs = []string{}
		}
		if ips == nil {
			ips = []string{}
		}

		rows, err := db.Query(context.Background(), searchSQL, macs, ips)
		if err != nil {
			log.Printf("search: query: %v", err)
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
				&res.AccessVlan, &res.AccessVlanName,
				&res.VoiceVlan, &res.VoiceVlanName,
			); err != nil {
				log.Printf("search: scan: %v", err)
				continue
			}
			results = append(results, res)
		}
		if err := rows.Err(); err != nil {
			log.Printf("search: rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		foundMACs := make(map[string]bool, len(results))
		foundIPs := make(map[string]bool, len(results))
		for _, res := range results {
			foundMACs[strings.ToLower(res.MacAddress)] = true
			if res.IPAddress != nil {
				foundIPs[*res.IPAddress] = true
			}
		}

		notFound := []string{}
		for _, tr := range termRecords {
			matched := false
			switch tr.kind {
			case "mac":
				for _, norm := range tr.lookFor {
					if foundMACs[norm] {
						matched = true
						break
					}
				}
			case "ip":
				matched = foundIPs[tr.lookFor[0]]
			case "hostname":
				for _, ip := range tr.lookFor {
					if foundIPs[ip] {
						matched = true
						break
					}
				}
			}
			if !matched {
				notFound = append(notFound, tr.original)
			}
		}

		writeResp(SearchResponse{Results: results, NotFound: notFound})
	}
}
