package modtypes

import "net/http"

// NavLink describes a navigation entry a module contributes to the shell layout.
type NavLink struct {
	Label     string
	Href      string
	AdminOnly bool
	NewTab    bool
	Header    bool // true → header button row; false → user dropdown menu
}

// Fragment is the portion of an OpenAPI 3 spec a module contributes.
type Fragment struct {
	Paths   map[string]any
	Schemas map[string]any
}

// Module is the contract every feature module must satisfy.
type Module interface {
	Routes(mux *http.ServeMux, auth, adminAuth func(http.Handler) http.Handler)
	OpenAPIFragment() Fragment
	NavLinks() []NavLink
}
