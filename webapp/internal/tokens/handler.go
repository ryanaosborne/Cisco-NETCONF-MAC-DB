package tokens

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ListHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := SAMLUser(r)
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

		list := []tokenInfo{}
		for rows.Next() {
			var t tokenInfo
			if err := rows.Scan(&t.ID, &t.Name, &t.Hint, &t.CreatedAt, &t.ExpiresAt, &t.LastUsedAt, &t.Revoked); err != nil {
				log.Printf("tokens: scan: %v", err)
				continue
			}
			list = append(list, t)
		}
		if err := rows.Err(); err != nil {
			log.Printf("tokens: rows: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(list)
	}
}

func CreateHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := SAMLUser(r)

		var req struct {
			Name        string `json:"name"`
			ExpiresDays *int   `json:"expires_days"`
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
			"token":      plaintext,
			"hint":       hint,
			"created_at": createdAt,
			"expires_at": expiresAt,
		})
	}
}

func RevokeHandler(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := SAMLUser(r)
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
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
