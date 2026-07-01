package tokens

import (
	"fmt"
	"net/http"

	"telemetry/webapp/internal/modtypes"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Module struct {
	db       *pgxpool.Pool
	pageHTML string
}

func New(db *pgxpool.Pool, pageHTML string) *Module {
	return &Module{db: db, pageHTML: pageHTML}
}

func (m *Module) Routes(mux *http.ServeMux, _, adminAuth func(http.Handler) http.Handler) {
	mux.Handle("/tokens", adminAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, m.pageHTML)
	})))
	mux.Handle("GET /api/tokens", adminAuth(ListHandler(m.db)))
	mux.Handle("POST /api/tokens", adminAuth(CreateHandler(m.db)))
	mux.Handle("DELETE /api/tokens/{id}", adminAuth(RevokeHandler(m.db)))
}

func (m *Module) NavLinks() []modtypes.NavLink {
	return []modtypes.NavLink{
		{Label: "API Tokens", Href: "/tokens", AdminOnly: true},
	}
}

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{}
}
