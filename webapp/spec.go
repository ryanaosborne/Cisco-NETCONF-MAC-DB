package main

import (
	"encoding/json"

	"telemetry/webapp/internal/modtypes"
)

// buildOpenAPISpec merges each module's path and schema fragments into a
// complete OpenAPI 3 document and returns it as JSON.
func buildOpenAPISpec(modules []modtypes.Module) []byte {
	paths := map[string]any{}
	schemas := map[string]any{}

	for _, m := range modules {
		frag := m.OpenAPIFragment()
		for k, v := range frag.Paths {
			paths[k] = v
		}
		for k, v := range frag.Schemas {
			schemas[k] = v
		}
	}

	spec := map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "PortFinder API",
			"description": "Look up where a device is connected on the network. Query Cisco IOS-XE telemetry data by MAC address or IP address to find the switch port, VLAN, and interface details.\n\n**Authentication:** API requests are authenticated either by your browser SAML session (when calling from this docs page) or by a personal access token sent as `Authorization: Bearer pfk_...`. Generate and manage tokens on the [API Tokens](/tokens) page.",
			"version":     "1.0.1",
		},
		"servers": []any{
			map[string]any{"url": "/", "description": "This server"},
		},
		"paths": paths,
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{
					"type":        "http",
					"scheme":      "bearer",
					"description": "Personal access token generated on the /tokens page (format: pfk_...). Alternative to the SAML browser session.",
				},
			},
			"schemas": schemas,
		},
	}

	out, _ := json.MarshalIndent(spec, "", "  ")
	return out
}
