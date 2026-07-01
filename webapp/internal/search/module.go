package search

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
	mux.Handle("/api/search", auth(Handler(m.db)))
}

func (m *Module) NavLinks() []modtypes.NavLink { return nil }

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{
		Paths: map[string]any{
			"/api/search": map[string]any{
				"post": map[string]any{
					"summary":     "Search by MAC or IP address",
					"description": "Returns all matching rows from the telemetry database. Each term is classified as a MAC address or IP address and looked up accordingly. A single request may mix MACs and IPs.\n\n**Accepted MAC formats** (all case-insensitive):\n- Colon-separated: `aa:bb:cc:dd:ee:ff`\n- Hyphen-separated: `aa-bb-cc-dd-ee-ff`\n- Cisco dot notation: `aabb.ccdd.eeff`\n- Bare hex: `aabbccddeeff`",
					"operationId": "search",
					"security":    []any{map[string]any{"bearerAuth": []any{}}, map[string]any{}},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{"$ref": "#/components/schemas/SearchRequest"},
								"examples": map[string]any{
									"single-mac": map[string]any{"summary": "Single MAC address", "value": map[string]any{"terms": []string{"aa:bb:cc:dd:ee:ff"}}},
									"single-ip":  map[string]any{"summary": "Single IP address", "value": map[string]any{"terms": []string{"10.0.0.203"}}},
									"mixed":      map[string]any{"summary": "Mixed MACs and IPs", "value": map[string]any{"terms": []string{"aa:bb:cc:dd:ee:ff", "10.0.0.203", "4cab4f8b9902"}}},
									"cisco-dot":  map[string]any{"summary": "Cisco dot-notation MAC", "value": map[string]any{"terms": []string{"aabb.ccdd.eeff"}}},
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Matching telemetry records. Returns an empty array when no matches are found.",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{"type": "array", "items": map[string]any{"$ref": "#/components/schemas/TelemetryRecord"}},
								},
							},
						},
						"400": map[string]any{"description": "Request body is not valid JSON.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"401": map[string]any{"description": "Bearer token is invalid, expired, or revoked.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"500": map[string]any{"description": "Database error.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
					},
				},
			},
		},
		Schemas: map[string]any{
			"SearchRequest": map[string]any{
				"type":     "object",
				"required": []string{"terms"},
				"properties": map[string]any{
					"terms": map[string]any{
						"type":        "array",
						"minItems":    1,
						"items":       map[string]any{"type": "string"},
						"description": "MAC addresses and/or IP addresses to look up.",
						"example":     []string{"aa:bb:cc:dd:ee:ff", "10.0.0.203"},
					},
				},
			},
			"TelemetryRecord": map[string]any{
				"type":     "object",
				"required": []string{"node_id", "mac_address"},
				"properties": map[string]any{
					"node_id":               map[string]any{"type": "string", "description": "Device hostname or ID reported by the telemetry stream.", "example": "switch1.example.com"},
					"mac_address":           map[string]any{"type": "string", "description": "MAC address as stored by the device.", "example": "aa:bb:cc:dd:ee:ff"},
					"ip_address":            map[string]any{"type": "string", "nullable": true, "description": "IP address from the ARP table.", "example": "10.0.0.203"},
					"interface":             map[string]any{"type": "string", "nullable": true, "description": "Full interface name.", "example": "GigabitEthernet1/0/4"},
					"interface_description": map[string]any{"type": "string", "nullable": true, "description": "Configured interface description.", "example": "User access port"},
					"access_vlan":           map[string]any{"type": "integer", "nullable": true, "description": "Access (data) VLAN configured on the interface.", "example": 100},
					"access_vlan_name":      map[string]any{"type": "string", "nullable": true, "description": "Name of the access (data) VLAN.", "example": "USERS"},
					"voice_vlan":            map[string]any{"type": "integer", "nullable": true, "description": "Voice VLAN configured on the interface.", "example": 200},
					"voice_vlan_name":       map[string]any{"type": "string", "nullable": true, "description": "Name of the voice VLAN.", "example": "VOICE"},
				},
			},
		},
	}
}
