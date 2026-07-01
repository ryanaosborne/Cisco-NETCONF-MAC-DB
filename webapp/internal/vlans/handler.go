package vlans

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

const vlansSQL = `
SELECT vlan_id, name
FROM vlan_table
WHERE node_id = $1
ORDER BY vlan_id
`

func Handler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		nodeID := r.URL.Query().Get("node_id")
		if nodeID == "" {
			http.Error(w, "node_id query parameter is required", http.StatusBadRequest)
			return
		}

		rows, err := db.Query(context.Background(), vlansSQL, nodeID)
		if err != nil {
			log.Printf("vlans: query: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		result := []VlanRecord{}
		for rows.Next() {
			var v VlanRecord
			if err := rows.Scan(&v.VlanID, &v.Name); err != nil {
				log.Printf("vlans: scan: %v", err)
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}
			result = append(result, v)
		}
		if err := rows.Err(); err != nil {
			log.Printf("vlans: rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(VlansResponse{Vlans: result})
	}
}
