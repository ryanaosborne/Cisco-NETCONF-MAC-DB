package swagger

import (
	"fmt"
	"net/http"

	"telemetry/webapp/internal/modtypes"
)

type Module struct {
	pageHTML string
	specJSON []byte
}

// New creates the swagger module. specJSON is the fully assembled OpenAPI spec
// produced by main after merging all data module fragments.
func New(pageHTML string, specJSON []byte) *Module {
	return &Module{pageHTML: pageHTML, specJSON: specJSON}
}

func (m *Module) Routes(mux *http.ServeMux, _, adminAuth func(http.Handler) http.Handler) {
	mux.Handle("/swagger", adminAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, m.pageHTML)
	})))
	mux.Handle("/api/openapi.json", adminAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(m.specJSON)
	})))
}

func (m *Module) NavLinks() []modtypes.NavLink {
	return []modtypes.NavLink{
		{Label: "API Docs", Href: "/swagger", AdminOnly: true, NewTab: true},
	}
}

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{}
}
