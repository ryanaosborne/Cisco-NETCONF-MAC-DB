package dbview

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const dbInspectLimit = 500

func InspectHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		var resp dbInspectResponse

		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM mac_table`).Scan(&resp.MacTotal); err != nil {
			log.Printf("dbinspect: count mac: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		macRows, err := db.Query(ctx, `SELECT id,node_id,mac_address,interface,vlan,mac_type,collected_at FROM mac_table ORDER BY id DESC LIMIT $1`, dbInspectLimit)
		if err != nil {
			log.Printf("dbinspect: query mac: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		resp.MacRows = []macRow{}
		for macRows.Next() {
			var row macRow
			if err := macRows.Scan(&row.ID, &row.NodeID, &row.MacAddress, &row.Interface, &row.Vlan, &row.MacType, &row.CollectedAt); err != nil {
				log.Printf("dbinspect: scan mac: %v", err)
				continue
			}
			resp.MacRows = append(resp.MacRows, row)
		}
		if err := macRows.Err(); err != nil {
			log.Printf("dbinspect: rows mac: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		macRows.Close()

		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM arp_table`).Scan(&resp.ArpTotal); err != nil {
			log.Printf("dbinspect: count arp: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		arpRows, err := db.Query(ctx, `SELECT id,node_id,ip_address,mac_address,interface,age_seconds,collected_at FROM arp_table ORDER BY id DESC LIMIT $1`, dbInspectLimit)
		if err != nil {
			log.Printf("dbinspect: query arp: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		resp.ArpRows = []arpRow{}
		for arpRows.Next() {
			var row arpRow
			if err := arpRows.Scan(&row.ID, &row.NodeID, &row.IPAddress, &row.MacAddress, &row.Interface, &row.AgeSeconds, &row.CollectedAt); err != nil {
				log.Printf("dbinspect: scan arp: %v", err)
				continue
			}
			resp.ArpRows = append(resp.ArpRows, row)
		}
		if err := arpRows.Err(); err != nil {
			log.Printf("dbinspect: rows arp: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		arpRows.Close()

		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM interface_table`).Scan(&resp.InterfaceTotal); err != nil {
			log.Printf("dbinspect: count interface: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		ifaceRows, err := db.Query(ctx, `SELECT id,node_id,name,description,shutdown,ip_address,prefix_len,vrf,mtu,access_vlan,voice_vlan,collected_at FROM interface_table ORDER BY id DESC LIMIT $1`, dbInspectLimit)
		if err != nil {
			log.Printf("dbinspect: query interface: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		resp.InterfaceRows = []interfaceRow{}
		for ifaceRows.Next() {
			var row interfaceRow
			if err := ifaceRows.Scan(&row.ID, &row.NodeID, &row.Name, &row.Description, &row.Shutdown, &row.IPAddress, &row.PrefixLen, &row.VRF, &row.MTU, &row.AccessVlan, &row.VoiceVlan, &row.CollectedAt); err != nil {
				log.Printf("dbinspect: scan interface: %v", err)
				continue
			}
			resp.InterfaceRows = append(resp.InterfaceRows, row)
		}
		if err := ifaceRows.Err(); err != nil {
			log.Printf("dbinspect: rows interface: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		ifaceRows.Close()

		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM vlan_table`).Scan(&resp.VlanTotal); err != nil {
			log.Printf("dbinspect: count vlan: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		vlanRows, err := db.Query(ctx, `SELECT id,node_id,vlan_id,name,status,collected_at FROM vlan_table ORDER BY id DESC LIMIT $1`, dbInspectLimit)
		if err != nil {
			log.Printf("dbinspect: query vlan: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		resp.VlanRows = []vlanRow{}
		for vlanRows.Next() {
			var row vlanRow
			if err := vlanRows.Scan(&row.ID, &row.NodeID, &row.VlanID, &row.Name, &row.Status, &row.CollectedAt); err != nil {
				log.Printf("dbinspect: scan vlan: %v", err)
				continue
			}
			resp.VlanRows = append(resp.VlanRows, row)
		}
		if err := vlanRows.Err(); err != nil {
			log.Printf("dbinspect: rows vlan: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		vlanRows.Close()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func TableHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		tbl := r.URL.Query().Get("table")
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		size, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if size < 1 || size > 1000 {
			size = 100
		}
		offset := (page - 1) * size
		pat := "%" + q + "%"
		hasQ := q != ""

		out := tableResp{Page: page, PageSize: size}

		switch tbl {
		case "mac":
			if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM mac_table`).Scan(&out.Total); err != nil {
				log.Printf("dbinspect-table mac total: %v", err)
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}
			macWhere := `node_id ILIKE $1 OR mac_address ILIKE $1 OR COALESCE(interface,'') ILIKE $1 OR COALESCE(vlan::text,'') ILIKE $1 OR COALESCE(mac_type,'') ILIKE $1`
			var result []macRow
			if hasQ {
				if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM mac_table WHERE `+macWhere, pat).Scan(&out.FilteredTotal); err != nil {
					log.Printf("dbinspect-table mac filtered: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows, err := db.Query(ctx, `SELECT id,node_id,mac_address,interface,vlan,mac_type,collected_at FROM mac_table WHERE `+macWhere+` ORDER BY id DESC LIMIT $2 OFFSET $3`, pat, size, offset)
				if err != nil {
					log.Printf("dbinspect-table mac data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row macRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.MacAddress, &row.Interface, &row.Vlan, &row.MacType, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table mac scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table mac rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			} else {
				out.FilteredTotal = out.Total
				rows, err := db.Query(ctx, `SELECT id,node_id,mac_address,interface,vlan,mac_type,collected_at FROM mac_table ORDER BY id DESC LIMIT $1 OFFSET $2`, size, offset)
				if err != nil {
					log.Printf("dbinspect-table mac data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row macRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.MacAddress, &row.Interface, &row.Vlan, &row.MacType, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table mac scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table mac rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
			if result == nil {
				result = []macRow{}
			}
			out.Rows = result

		case "arp":
			if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM arp_table`).Scan(&out.Total); err != nil {
				log.Printf("dbinspect-table arp total: %v", err)
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}
			arpWhere := `node_id ILIKE $1 OR ip_address ILIKE $1 OR COALESCE(mac_address,'') ILIKE $1 OR COALESCE(interface,'') ILIKE $1`
			var result []arpRow
			if hasQ {
				if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM arp_table WHERE `+arpWhere, pat).Scan(&out.FilteredTotal); err != nil {
					log.Printf("dbinspect-table arp filtered: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows, err := db.Query(ctx, `SELECT id,node_id,ip_address,mac_address,interface,age_seconds,collected_at FROM arp_table WHERE `+arpWhere+` ORDER BY id DESC LIMIT $2 OFFSET $3`, pat, size, offset)
				if err != nil {
					log.Printf("dbinspect-table arp data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row arpRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.IPAddress, &row.MacAddress, &row.Interface, &row.AgeSeconds, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table arp scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table arp rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			} else {
				out.FilteredTotal = out.Total
				rows, err := db.Query(ctx, `SELECT id,node_id,ip_address,mac_address,interface,age_seconds,collected_at FROM arp_table ORDER BY id DESC LIMIT $1 OFFSET $2`, size, offset)
				if err != nil {
					log.Printf("dbinspect-table arp data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row arpRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.IPAddress, &row.MacAddress, &row.Interface, &row.AgeSeconds, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table arp scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table arp rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
			if result == nil {
				result = []arpRow{}
			}
			out.Rows = result

		case "interface":
			if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM interface_table`).Scan(&out.Total); err != nil {
				log.Printf("dbinspect-table interface total: %v", err)
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}
			ifaceWhere := `node_id ILIKE $1 OR name ILIKE $1 OR COALESCE(description,'') ILIKE $1 OR COALESCE(ip_address,'') ILIKE $1 OR COALESCE(vrf,'') ILIKE $1`
			var result []interfaceRow
			if hasQ {
				if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM interface_table WHERE `+ifaceWhere, pat).Scan(&out.FilteredTotal); err != nil {
					log.Printf("dbinspect-table interface filtered: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows, err := db.Query(ctx, `SELECT id,node_id,name,description,shutdown,ip_address,prefix_len,vrf,mtu,access_vlan,voice_vlan,collected_at FROM interface_table WHERE `+ifaceWhere+` ORDER BY id DESC LIMIT $2 OFFSET $3`, pat, size, offset)
				if err != nil {
					log.Printf("dbinspect-table interface data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row interfaceRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.Name, &row.Description, &row.Shutdown, &row.IPAddress, &row.PrefixLen, &row.VRF, &row.MTU, &row.AccessVlan, &row.VoiceVlan, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table interface scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table interface rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			} else {
				out.FilteredTotal = out.Total
				rows, err := db.Query(ctx, `SELECT id,node_id,name,description,shutdown,ip_address,prefix_len,vrf,mtu,access_vlan,voice_vlan,collected_at FROM interface_table ORDER BY id DESC LIMIT $1 OFFSET $2`, size, offset)
				if err != nil {
					log.Printf("dbinspect-table interface data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row interfaceRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.Name, &row.Description, &row.Shutdown, &row.IPAddress, &row.PrefixLen, &row.VRF, &row.MTU, &row.AccessVlan, &row.VoiceVlan, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table interface scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table interface rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
			if result == nil {
				result = []interfaceRow{}
			}
			out.Rows = result

		case "vlan":
			if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM vlan_table`).Scan(&out.Total); err != nil {
				log.Printf("dbinspect-table vlan total: %v", err)
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}
			vlanWhere := `node_id ILIKE $1 OR vlan_id::text ILIKE $1 OR COALESCE(name,'') ILIKE $1 OR COALESCE(status,'') ILIKE $1`
			var result []vlanRow
			if hasQ {
				if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM vlan_table WHERE `+vlanWhere, pat).Scan(&out.FilteredTotal); err != nil {
					log.Printf("dbinspect-table vlan filtered: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows, err := db.Query(ctx, `SELECT id,node_id,vlan_id,name,status,collected_at FROM vlan_table WHERE `+vlanWhere+` ORDER BY id DESC LIMIT $2 OFFSET $3`, pat, size, offset)
				if err != nil {
					log.Printf("dbinspect-table vlan data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row vlanRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.VlanID, &row.Name, &row.Status, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table vlan scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table vlan rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			} else {
				out.FilteredTotal = out.Total
				rows, err := db.Query(ctx, `SELECT id,node_id,vlan_id,name,status,collected_at FROM vlan_table ORDER BY id DESC LIMIT $1 OFFSET $2`, size, offset)
				if err != nil {
					log.Printf("dbinspect-table vlan data: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				for rows.Next() {
					var row vlanRow
					if err := rows.Scan(&row.ID, &row.NodeID, &row.VlanID, &row.Name, &row.Status, &row.CollectedAt); err != nil {
						log.Printf("dbinspect-table vlan scan: %v", err)
						continue
					}
					result = append(result, row)
				}
				if err := rows.Err(); err != nil {
					log.Printf("dbinspect-table vlan rows: %v", err)
					http.Error(w, "database error", http.StatusInternalServerError)
					return
				}
				rows.Close()
			}
			if result == nil {
				result = []vlanRow{}
			}
			out.Rows = result

		default:
			http.Error(w, "invalid table", http.StatusBadRequest)
			return
		}

		out.Pages = (out.FilteredTotal + size - 1) / size
		if out.Pages < 1 {
			out.Pages = 1
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}
