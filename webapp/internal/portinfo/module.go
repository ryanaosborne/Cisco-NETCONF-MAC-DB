package portinfo

import (
	"net/http"

	"telemetry/webapp/internal/modtypes"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Module struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Module {
	return &Module{db: db}
}

func (m *Module) Routes(mux *http.ServeMux, auth, _ func(http.Handler) http.Handler) {
	mux.Handle("/api/port", auth(Handler(m.db)))
}

func (m *Module) NavLinks() []modtypes.NavLink { return nil }

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{
		Paths: map[string]any{
			"/api/port": map[string]any{
				"get": map[string]any{
					"summary":     "Get port details by switch and port name",
					"description": "Returns all stored information for a single interface: description, shutdown state, IP address, VRF, MTU, access VLAN (with name), and voice VLAN (with name).",
					"operationId": "getPort",
					"security":    []any{map[string]any{"bearerAuth": []any{}}, map[string]any{}},
					"parameters": []any{
						map[string]any{
							"name":        "node_id",
							"in":          "query",
							"required":    true,
							"description": "Switch hostname or node ID.",
							"schema":      map[string]any{"type": "string"},
							"example":     "switch1.example.com",
						},
						map[string]any{
							"name":        "port",
							"in":          "query",
							"required":    true,
							"description": "Full interface name as stored by the device.",
							"schema":      map[string]any{"type": "string"},
							"example":     "GigabitEthernet1/0/4",
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Port details.",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{"$ref": "#/components/schemas/PortRecord"},
								},
							},
						},
						"400": map[string]any{"description": "Missing node_id or port parameter.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"401": map[string]any{"description": "Bearer token is invalid, expired, or revoked.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"404": map[string]any{"description": "No matching interface found.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"500": map[string]any{"description": "Database error.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
					},
				},
			},
		},
		Schemas: map[string]any{
			"PortRecord": map[string]any{
				"type":     "object",
				"required": []string{"node_id", "name", "shutdown", "access_vlan", "collected_at"},
				"properties": map[string]any{
					"node_id":          map[string]any{"type": "string", "description": "Switch hostname or node ID.", "example": "switch1.example.com"},
					"name":             map[string]any{"type": "string", "description": "Full interface name.", "example": "GigabitEthernet1/0/4"},
					"description":      map[string]any{"type": "string", "nullable": true, "description": "Configured interface description.", "example": "User access port"},
					"shutdown":         map[string]any{"type": "boolean", "description": "True if the interface is administratively shut down.", "example": false},
					"ip_address":       map[string]any{"type": "string", "nullable": true, "description": "IP address configured on the interface (routed ports).", "example": "10.0.0.1"},
					"prefix_len":       map[string]any{"type": "integer", "nullable": true, "description": "Prefix length of the interface IP address.", "example": 24},
					"vrf":              map[string]any{"type": "string", "nullable": true, "description": "VRF the interface is assigned to.", "example": "MGMT"},
					"mtu":              map[string]any{"type": "integer", "nullable": true, "description": "Interface MTU.", "example": 1500},
					"access_vlan":      map[string]any{"type": "integer", "description": "Access (data) VLAN configured on the interface.", "example": 100},
					"access_vlan_name": map[string]any{"type": "string", "nullable": true, "description": "Name of the access VLAN.", "example": "USERS"},
					"voice_vlan":       map[string]any{"type": "integer", "nullable": true, "description": "Voice VLAN configured on the interface.", "example": 200},
					"voice_vlan_name":  map[string]any{"type": "string", "nullable": true, "description": "Name of the voice VLAN.", "example": "VOICE"},
					"collected_at":     map[string]any{"type": "string", "format": "date-time", "description": "Timestamp of the last telemetry collection for this interface.", "example": "2024-01-15T10:30:00Z"},
				},
			},
		},
	}
}
