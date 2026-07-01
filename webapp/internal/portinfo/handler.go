package portinfo

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const portSQL = `
SELECT
    i.node_id,
    i.name,
    i.description,
    i.shutdown,
    i.ip_address,
    i.prefix_len,
    i.vrf,
    i.mtu,
    i.access_vlan,
    dv.name        AS access_vlan_name,
    i.voice_vlan,
    vv.name        AS voice_vlan_name,
    i.collected_at
FROM interface_table i
LEFT JOIN vlan_table dv ON i.node_id = dv.node_id AND i.access_vlan = dv.vlan_id
LEFT JOIN vlan_table vv ON i.node_id = vv.node_id AND i.voice_vlan  = vv.vlan_id
WHERE i.node_id = $1 AND i.name = $2
LIMIT 1
`

func Handler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		nodeID := r.URL.Query().Get("node_id")
		port := r.URL.Query().Get("port")
		if nodeID == "" || port == "" {
			http.Error(w, "node_id and port query parameters are required", http.StatusBadRequest)
			return
		}

		row := db.QueryRow(context.Background(), portSQL, nodeID, port)

		var res PortResult
		var collectedAt time.Time
		if err := row.Scan(
			&res.NodeID, &res.Name, &res.Description, &res.Shutdown,
			&res.IPAddress, &res.PrefixLen, &res.VRF, &res.MTU,
			&res.AccessVlan, &res.AccessVlanName,
			&res.VoiceVlan, &res.VoiceVlanName,
			&collectedAt,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			log.Printf("portinfo: scan: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		res.CollectedAt = collectedAt.UTC().Format(time.RFC3339)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	}
}
