package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/crewjam/saml/samlsp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// API tokens are GitHub-style personal access tokens: 32 bytes of CSPRNG
// entropy shown to the user exactly once. Only the SHA-256 hash is stored,
// so a database leak does not leak usable credentials. The "pfk_" prefix
// makes tokens recognisable in logs and secret scanners.
const (
	apiTokenPrefix    = "pfk_"
	maxActiveTokens   = 10
	maxTokenNameLen   = 100
	defaultExpiryDays = 90
	maxExpiryDays     = 730
)

const createTokenTableSQL = `
CREATE TABLE IF NOT EXISTS api_tokens (
    id           BIGSERIAL PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    token_hash   TEXT        NOT NULL UNIQUE,
    token_hint   TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS api_tokens_user ON api_tokens (user_id);
`

// ensureTokenTable creates the api_tokens table when it does not exist yet.
// Idempotent, so it is safe on every startup and covers deployments whose
// postgres volume predates the api_tokens table in migrations/001_init.sql
// (the init scripts in docker-entrypoint-initdb.d only run on a fresh volume).
func ensureTokenTable(db *pgxpool.Pool) {
	if _, err := db.Exec(context.Background(), createTokenTableSQL); err != nil {
		log.Fatalf("tokens: create api_tokens table: %v", err)
	}
}

// samlUser returns a stable identity for the logged-in browser user. It
// prefers common IdP attributes (eduPersonPrincipalName / mail and their
// OID forms), then falls back to the SAML NameID carried as the session
// JWT subject. Returns "local" when SAML is disabled (development mode).
func samlUser(r *http.Request) string {
	for _, attr := range []string{
		// Azure AD / Entra ID claim URIs
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
		// Shibboleth / generic IdP attribute names
		"eduPersonPrincipalName",
		"urn:oid:1.3.6.1.4.1.5923.1.1.1.6", // eduPersonPrincipalName
		"mail",
		"urn:oid:0.9.2342.19200300.100.1.3", // mail
		"email",
		"uid",
	} {
		if v := samlsp.AttributeFromContext(r.Context(), attr); v != "" {
			return v
		}
	}
	if s := samlsp.SessionFromContext(r.Context()); s != nil {
		if jc, ok := s.(samlsp.JWTSessionClaims); ok && jc.Subject != "" {
			return jc.Subject
		}
	}
	return "local"
}

// generateToken returns the plaintext token, its SHA-256 hex digest for
// storage, and a short display hint (prefix + first 4 chars).
func generateToken() (plaintext, hash, hint string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", "", err
	}
	plaintext = apiTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(plaintext))
	hash = hex.EncodeToString(sum[:])
	hint = plaintext[:len(apiTokenPrefix)+4] + "…"
	return plaintext, hash, hint, nil
}

// lookupToken validates a presented bearer token and returns the owning
// user. Lookup is by hash, so the plaintext never touches the database or
// its logs. last_used_at is updated best-effort off the request path.
func lookupToken(ctx context.Context, db *pgxpool.Pool, plaintext string) (string, error) {
	sum := sha256.Sum256([]byte(plaintext))
	hash := hex.EncodeToString(sum[:])

	var id int64
	var user string
	err := db.QueryRow(ctx, `
		SELECT id, user_id FROM api_tokens
		WHERE token_hash = $1
		  AND revoked_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())`, hash).Scan(&id, &user)
	if err != nil {
		return "", err
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := db.Exec(ctx, `UPDATE api_tokens SET last_used_at = now() WHERE id = $1`, id); err != nil {
			log.Printf("tokens: update last_used_at: %v", err)
		}
	}()
	return user, nil
}

// bearerOrSAML protects data endpoints. A request carrying a syntactically
// valid bearer token is authenticated against api_tokens and never touches
// the SAML flow; an invalid token gets a 401 (API clients cannot follow IdP
// redirects). Requests without a bearer token fall through to the regular
// SAML session check.
func bearerOrSAML(db *pgxpool.Pool, saml *samlsp.Middleware) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				tok := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
				if strings.HasPrefix(tok, apiTokenPrefix) {
					if _, err := lookupToken(r.Context(), db, tok); err == nil {
						next.ServeHTTP(w, r)
						return
					} else if !errors.Is(err, pgx.ErrNoRows) {
						log.Printf("tokens: lookup: %v", err)
						http.Error(w, "database error", http.StatusInternalServerError)
						return
					}
					w.Header().Set("WWW-Authenticate", `Bearer realm="portfinder"`)
					http.Error(w, "invalid, expired, or revoked token", http.StatusUnauthorized)
					return
				}
			}
			if saml == nil {
				next.ServeHTTP(w, r)
				return
			}
			saml.RequireAccount(next).ServeHTTP(w, r)
		})
	}
}

type tokenInfo struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	Hint       string     `json:"hint"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	Revoked    bool       `json:"revoked"`
}

func handleTokenList(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := samlUser(r)
		rows, err := db.Query(r.Context(), `
			SELECT id, name, token_hint, created_at, expires_at, last_used_at, revoked_at IS NOT NULL
			FROM api_tokens
			WHERE user_id = $1
			ORDER BY created_at DESC`, user)
		if err != nil {
			log.Printf("tokens: list: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		tokens := []tokenInfo{}
		for rows.Next() {
			var t tokenInfo
			if err := rows.Scan(&t.ID, &t.Name, &t.Hint, &t.CreatedAt, &t.ExpiresAt, &t.LastUsedAt, &t.Revoked); err != nil {
				log.Printf("tokens: scan: %v", err)
				continue
			}
			tokens = append(tokens, t)
		}
		if err := rows.Err(); err != nil {
			log.Printf("tokens: rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tokens)
	}
}

func handleTokenCreate(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := samlUser(r)

		var req struct {
			Name        string `json:"name"`
			ExpiresDays *int   `json:"expires_days"` // omitted = 90, 0 = never
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		if req.Name == "" || len(req.Name) > maxTokenNameLen {
			http.Error(w, "name is required (max 100 characters)", http.StatusBadRequest)
			return
		}

		days := defaultExpiryDays
		if req.ExpiresDays != nil {
			days = *req.ExpiresDays
		}
		if days < 0 || days > maxExpiryDays {
			http.Error(w, "expires_days must be between 0 (never) and 730", http.StatusBadRequest)
			return
		}
		var expiresAt *time.Time
		if days > 0 {
			t := time.Now().AddDate(0, 0, days)
			expiresAt = &t
		}

		var active int
		err := db.QueryRow(r.Context(), `
			SELECT COUNT(*) FROM api_tokens
			WHERE user_id = $1 AND revoked_at IS NULL
			  AND (expires_at IS NULL OR expires_at > now())`, user).Scan(&active)
		if err != nil {
			log.Printf("tokens: count: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		if active >= maxActiveTokens {
			http.Error(w, "token limit reached: revoke an existing token first", http.StatusConflict)
			return
		}

		plaintext, hash, hint, err := generateToken()
		if err != nil {
			log.Printf("tokens: generate: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		var id int64
		var createdAt time.Time
		err = db.QueryRow(r.Context(), `
			INSERT INTO api_tokens (user_id, name, token_hash, token_hint, expires_at)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id, created_at`, user, req.Name, hash, hint, expiresAt).Scan(&id, &createdAt)
		if err != nil {
			log.Printf("tokens: insert: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"id":         id,
			"name":       req.Name,
			"token":      plaintext, // only time the plaintext is ever returned
			"hint":       hint,
			"created_at": createdAt,
			"expires_at": expiresAt,
		})
	}
}

func handleTokenRevoke(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := samlUser(r)
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// Scoped to user_id so users can only revoke their own tokens.
		tag, err := db.Exec(r.Context(), `
			UPDATE api_tokens SET revoked_at = now()
			WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`, id, user)
		if err != nil {
			log.Printf("tokens: revoke: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		if tag.RowsAffected() == 0 {
			http.Error(w, "token not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
