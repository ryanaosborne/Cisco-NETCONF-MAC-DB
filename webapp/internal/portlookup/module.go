package portlookup

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
	mux.Handle("/api/portlookup", auth(Handler(m.db)))
}

func (m *Module) NavLinks() []modtypes.NavLink { return nil }

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{
		Paths: map[string]any{
			"/api/portlookup": map[string]any{
				"post": map[string]any{
					"summary":     "Bulk port lookup by partial switch name and port descriptions",
					"description": "Returns all ports on matching switches whose description contains any of the provided strings. Both the switch name and each description are matched case-insensitively as substrings. Designed for bulk lookups — up to 1000 descriptions per request.",
					"operationId": "portLookup",
					"security":    []any{map[string]any{"bearerAuth": []any{}}, map[string]any{}},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{"$ref": "#/components/schemas/PortLookupRequest"},
								"examples": map[string]any{
									"bulk": map[string]any{
										"summary": "Lookup by partial switch name and descriptions",
										"value": map[string]any{
											"switch":       "core-sw",
											"descriptions": []string{"room 101", "lab printer", "conf room b"},
										},
									},
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Matched ports. Returns an empty array when nothing matches.",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{"$ref": "#/components/schemas/PortLookupResponse"},
								},
							},
						},
						"400": map[string]any{"description": "Missing or invalid request body.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"401": map[string]any{"description": "Bearer token is invalid, expired, or revoked.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"500": map[string]any{"description": "Database error.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
					},
				},
			},
		},
		Schemas: map[string]any{
			"PortLookupRequest": map[string]any{
				"type":     "object",
				"required": []string{"switch", "descriptions"},
				"properties": map[string]any{
					"switch": map[string]any{
						"type":        "string",
						"description": "Partial switch name to filter by (case-insensitive substring match).",
						"example":     "core-sw",
					},
					"descriptions": map[string]any{
						"type":        "array",
						"minItems":    1,
						"items":       map[string]any{"type": "string"},
						"description": "Port descriptions to match (each is a case-insensitive substring match).",
						"example":     []string{"room 101", "lab printer"},
					},
				},
			},
			"PortLookupResponse": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"results": map[string]any{
						"type":  "array",
						"items": map[string]any{"$ref": "#/components/schemas/PortLookupMatch"},
					},
				},
			},
			"PortLookupMatch": map[string]any{
				"type":     "object",
				"required": []string{"node_id", "interface"},
				"properties": map[string]any{
					"node_id":          map[string]any{"type": "string", "description": "Switch hostname.", "example": "core-sw-01.example.com"},
					"interface":        map[string]any{"type": "string", "description": "Full interface name.", "example": "GigabitEthernet1/0/4"},
					"description":      map[string]any{"type": "string", "nullable": true, "description": "Configured interface description.", "example": "room 101 - printer"},
					"access_vlan":      map[string]any{"type": "integer", "nullable": true, "description": "Access VLAN ID.", "example": 100},
					"access_vlan_name": map[string]any{"type": "string", "nullable": true, "description": "Access VLAN name.", "example": "USERS"},
					"voice_vlan":       map[string]any{"type": "integer", "nullable": true, "description": "Voice VLAN ID.", "example": 200},
					"voice_vlan_name":  map[string]any{"type": "string", "nullable": true, "description": "Voice VLAN name.", "example": "VOICE"},
				},
			},
		},
	}
}
