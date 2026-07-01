package tokens

import "time"

const (
	apiTokenPrefix    = "pfk_"
	maxActiveTokens   = 10
	maxTokenNameLen   = 100
	defaultExpiryDays = 90
	maxExpiryDays     = 730
)

type tokenInfo struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	Hint       string     `json:"hint"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	Revoked    bool       `json:"revoked"`
}
