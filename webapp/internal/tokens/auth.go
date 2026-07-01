package tokens

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/crewjam/saml/samlsp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// EnsureTable creates the api_tokens table when it does not exist yet. Idempotent.
func EnsureTable(db *pgxpool.Pool) {
	if _, err := db.Exec(context.Background(), createTokenTableSQL); err != nil {
		log.Fatalf("tokens: create api_tokens table: %v", err)
	}
}

// SAMLUser returns a stable identity for the logged-in browser user. Falls back
// to the SAML NameID, then "local" when SAML is disabled.
func SAMLUser(r *http.Request) string {
	for _, attr := range []string{
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
		"eduPersonPrincipalName",
		"urn:oid:1.3.6.1.4.1.5923.1.1.1.6",
		"mail",
		"urn:oid:0.9.2342.19200300.100.1.3",
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

// LookupToken validates a presented bearer token and returns the owning user.
// last_used_at is updated best-effort off the request path.
func LookupToken(ctx context.Context, db *pgxpool.Pool, plaintext string) (string, error) {
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

// BearerOrSAML protects data endpoints. A request carrying a syntactically
// valid bearer token is authenticated against api_tokens and never touches
// the SAML flow; an invalid token gets a 401. Requests without a bearer
// token fall through to the regular SAML session check.
func BearerOrSAML(db *pgxpool.Pool, saml *samlsp.Middleware) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				tok := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
				if strings.HasPrefix(tok, apiTokenPrefix) {
					if _, err := LookupToken(r.Context(), db, tok); err == nil {
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
