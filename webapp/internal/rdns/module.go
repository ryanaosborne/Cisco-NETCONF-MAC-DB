package rdns

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"telemetry/webapp/internal/modtypes"
)

var ipRe = regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}$`)

type Module struct{}

func New() *Module { return &Module{} }

func (m *Module) Routes(mux *http.ServeMux, auth, _ func(http.Handler) http.Handler) {
	mux.Handle("/api/rdns", auth(handler()))
}

func (m *Module) NavLinks() []modtypes.NavLink { return nil }

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{
		Paths: map[string]any{
			"/api/rdns": map[string]any{
				"get": map[string]any{
					"summary":     "Reverse DNS lookup for an IP address",
					"description": "Resolves an IPv4 address to a hostname via a reverse DNS (PTR) lookup. Returns the first hostname found with the trailing dot removed. The lookup times out after 3 seconds; on timeout or failure the response is still `200` with `hostname` set to `null`.",
					"operationId": "rdns",
					"security":    []any{map[string]any{"bearerAuth": []any{}}, map[string]any{}},
					"parameters": []any{
						map[string]any{
							"name":        "ip",
							"in":          "query",
							"required":    true,
							"schema":      map[string]any{"type": "string", "format": "ipv4"},
							"description": "IPv4 address in dotted-quad notation.",
							"example":     "10.0.0.203",
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Lookup completed. `hostname` is null when the address has no PTR record or the lookup failed.",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{"$ref": "#/components/schemas/RDNSResponse"},
								},
							},
						},
						"400": map[string]any{"description": "The `ip` parameter is missing or not a valid IPv4 address.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
						"401": map[string]any{"description": "Bearer token is invalid, expired, or revoked.", "content": map[string]any{"text/plain": map[string]any{"schema": map[string]any{"type": "string"}}}},
					},
				},
			},
		},
		Schemas: map[string]any{
			"RDNSResponse": map[string]any{
				"type":     "object",
				"required": []string{"hostname"},
				"properties": map[string]any{
					"hostname": map[string]any{
						"type":        "string",
						"nullable":    true,
						"description": "Hostname from the PTR record (trailing dot removed), or null when the lookup found nothing.",
						"example":     "workstation-42.example.com",
					},
				},
			},
		},
	}
}

func handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ip := r.URL.Query().Get("ip")
		if ip == "" || !ipRe.MatchString(ip) {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		names, err := net.DefaultResolver.LookupAddr(ctx, ip)

		w.Header().Set("Content-Type", "application/json")
		if err != nil || len(names) == 0 {
			json.NewEncoder(w).Encode(map[string]any{"hostname": nil})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"hostname": strings.TrimSuffix(names[0], ".")})
	}
}
