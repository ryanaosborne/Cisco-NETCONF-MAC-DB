package vlans

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
	mux.Handle("/api/vlans", auth(Handler(m.db)))
}

func (m *Module) NavLinks() []modtypes.NavLink { return nil }

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{
		Paths: map[string]any{
			"/api/vlans": map[string]any{
				"get": map[string]any{
					"summary":     "List VLANs on a switch",
					"description": "Returns all VLANs known for a given switch node, ordered by VLAN ID.",
					"operationId": "listVlans",
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
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "VLAN list (empty array when the switch has no recorded VLANs).",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{"$ref": "#/components/schemas/VlansResponse"},
								},
							},
						},
						"400": map[string]any{"description": "Missing node_id parameter.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"401": map[string]any{"description": "Bearer token is invalid, expired, or revoked.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"500": map[string]any{"description": "Database error.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
					},
				},
			},
		},
		Schemas: map[string]any{
			"VlansResponse": map[string]any{
				"type":     "object",
				"required": []string{"vlans"},
				"properties": map[string]any{
					"vlans": map[string]any{
						"type":  "array",
						"items": map[string]any{"$ref": "#/components/schemas/VlanRecord"},
					},
				},
			},
			"VlanRecord": map[string]any{
				"type":     "object",
				"required": []string{"vlan_id"},
				"properties": map[string]any{
					"vlan_id": map[string]any{"type": "integer", "description": "VLAN number.", "example": 100},
					"name":    map[string]any{"type": "string", "nullable": true, "description": "VLAN name as configured on the switch.", "example": "USERS"},
				},
			},
		},
	}
}
