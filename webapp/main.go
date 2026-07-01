package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"

	"telemetry/webapp/internal/dbview"
	"telemetry/webapp/internal/modtypes"
	"telemetry/webapp/internal/portinfo"
	"telemetry/webapp/internal/portlookup"
	"telemetry/webapp/internal/rdns"
	"telemetry/webapp/internal/vlans"
	"telemetry/webapp/internal/search"
	"telemetry/webapp/internal/swagger"
	"telemetry/webapp/internal/tokens"

	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlsp"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed views/index.html
var indexHTML string

//go:embed views/swagger.html
var swaggerHTML string

//go:embed views/dbview.html
var dbviewHTML string

//go:embed views/tokens.html
var tokensHTML string

func main() {
	dsn := envOr("POSTGRES_DSN", "postgres://telemetry:telemetry@localhost:15432/telemetry?sslmode=require")

	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(context.Background()); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	tokens.EnsureTable(db)

	samlMiddleware := setupSAML()

	// protect wraps a handler behind SAML when enabled; otherwise it's a no-op.
	protect := func(h http.Handler) http.Handler {
		if samlMiddleware == nil {
			return h
		}
		return samlMiddleware.RequireAccount(h)
	}

	adminRole := os.Getenv("SAML_ADMIN_ROLE")
	isAdmin := func(r *http.Request) bool {
		return hasAdminRole(r, samlMiddleware != nil, adminRole)
	}
	protectAdmin := func(h http.Handler) http.Handler {
		return protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isAdmin(r) {
				http.Error(w, "forbidden: this page requires the "+adminRole+" role", http.StatusForbidden)
				return
			}
			h.ServeHTTP(w, r)
		}))
	}
	if samlMiddleware != nil && adminRole != "" {
		log.Printf("saml: /tokens and /swagger restricted to role %q", adminRole)
	}

	// Data modules — always-on unless gated by their own env var check.
	apiAuth := tokens.BearerOrSAML(db, samlMiddleware)
	dataModules := []modtypes.Module{
		search.New(db),
		portinfo.New(db),
		portlookup.New(db),
		vlans.New(db),
		rdns.New(),
	}
	if os.Getenv("DBVIEW_ENABLED") == "true" {
		dataModules = append(dataModules, dbview.New(db, dbviewHTML))
	}

	// Build the aggregated OpenAPI spec before adding infra modules (which don't
	// contribute paths).
	specJSON := buildOpenAPISpec(dataModules)

	// Infra modules (admin-only pages; no API spec paths).
	infraModules := []modtypes.Module{
		tokens.New(db, tokensHTML),
		swagger.New(swaggerHTML, specJSON),
	}

	allModules := append(dataModules, infraModules...)

	// Collect nav links from all modules for per-request injection into the index template.
	var allNavLinks []modtypes.NavLink
	for _, m := range allModules {
		allNavLinks = append(allNavLinks, m.NavLinks()...)
	}

	indexTmpl := template.Must(template.New("index").Parse(indexHTML))

	mux := http.NewServeMux()

	if samlMiddleware != nil {
		mux.Handle("/saml/", samlMiddleware)
	}

	// Shell / index page — nav links are filtered per-request by admin status.
	mux.Handle("/", protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admin := isAdmin(r)
		var headerLinks, menuLinks []modtypes.NavLink
		for _, link := range allNavLinks {
			if link.AdminOnly && !admin {
				continue
			}
			if link.Header {
				headerLinks = append(headerLinks, link)
			} else {
				menuLinks = append(menuLinks, link)
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		indexTmpl.Execute(w, map[string]any{
			"HeaderLinks": headerLinks,
			"MenuLinks":   menuLinks,
		})
	})))

	// Identity for the header user menu. Browser-session only.
	mux.Handle("/api/me", protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"user": tokens.SAMLUser(r)})
	})))

	// Logout clears the local session cookie, then propagates to the IdP via SAML SLO.
	mux.Handle("/logout", protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if samlMiddleware == nil {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		nameID := ""
		if s := samlsp.SessionFromContext(r.Context()); s != nil {
			if jc, ok := s.(samlsp.JWTSessionClaims); ok {
				nameID = jc.Subject
			}
		}
		if err := samlMiddleware.Session.DeleteSession(w, r); err != nil {
			log.Printf("logout: delete session: %v", err)
		}
		if nameID != "" && samlMiddleware.ServiceProvider.GetSLOBindingLocation(saml.HTTPRedirectBinding) != "" {
			u, err := samlMiddleware.ServiceProvider.MakeRedirectLogoutRequest(nameID, "")
			if err == nil {
				http.Redirect(w, r, u.String(), http.StatusFound)
				return
			}
			log.Printf("logout: make IdP logout request: %v", err)
		}
		http.Redirect(w, r, "/", http.StatusFound)
	})))

	// Register all module routes.
	for _, m := range dataModules {
		m.Routes(mux, apiAuth, protectAdmin)
	}
	for _, m := range infraModules {
		m.Routes(mux, protect, protectAdmin)
	}

	addr := envOr("LISTEN_ADDR", ":8888")
	log.Printf("webapp listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
