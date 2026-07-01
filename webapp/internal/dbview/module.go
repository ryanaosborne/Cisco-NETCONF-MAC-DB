package dbview

import (
	"fmt"
	"log"
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

func (m *Module) Routes(mux *http.ServeMux, auth, _ func(http.Handler) http.Handler) {
	mux.Handle("/dbview", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, m.pageHTML)
	})))
	mux.Handle("/api/db-inspect", auth(InspectHandler(m.db)))
	mux.Handle("/api/db-inspect-table", auth(TableHandler(m.db)))
	log.Println("dbview: enabled")
}

func (m *Module) NavLinks() []modtypes.NavLink {
	return []modtypes.NavLink{
		{Label: "DB Inspector", Href: "/dbview", Header: true},
	}
}

func (m *Module) OpenAPIFragment() modtypes.Fragment {
	return modtypes.Fragment{}
}
