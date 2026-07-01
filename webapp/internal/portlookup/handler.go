package portlookup

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

const lookupSQL = `
SELECT
    i.node_id,
    i.name,
    i.description,
    i.access_vlan,
    dv.name  AS access_vlan_name,
    i.voice_vlan,
    vv.name  AS voice_vlan_name
FROM interface_table i
LEFT JOIN vlan_table dv ON i.node_id = dv.node_id AND i.access_vlan = dv.vlan_id
LEFT JOIN vlan_table vv ON i.node_id = vv.node_id AND i.voice_vlan  = vv.vlan_id
WHERE i.node_id      ILIKE $1
  AND i.description  ILIKE ANY($2::text[])
ORDER BY i.node_id, i.name
`

func Handler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req LookupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.Switch == "" {
			http.Error(w, "switch is required", http.StatusBadRequest)
			return
		}
		if len(req.Descriptions) == 0 {
			http.Error(w, "descriptions must not be empty", http.StatusBadRequest)
			return
		}

		switchPattern := "%" + req.Switch + "%"

		descPatterns := make([]string, len(req.Descriptions))
		for i, d := range req.Descriptions {
			descPatterns[i] = "%" + d + "%"
		}

		rows, err := db.Query(context.Background(), lookupSQL, switchPattern, descPatterns)
		if err != nil {
			log.Printf("portlookup: query: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		results := []PortMatch{}
		for rows.Next() {
			var m PortMatch
			if err := rows.Scan(
				&m.NodeID, &m.Interface, &m.Description,
				&m.AccessVlan, &m.AccessVlanName,
				&m.VoiceVlan, &m.VoiceVlanName,
			); err != nil {
				log.Printf("portlookup: scan: %v", err)
				continue
			}
			results = append(results, m)
		}
		if err := rows.Err(); err != nil {
			log.Printf("portlookup: rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LookupResponse{Results: results})
	}
}
