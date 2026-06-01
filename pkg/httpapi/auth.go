package httpapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
)

type AuthConfig struct {
	AdminTokens []string `yaml:"admin_tokens" json:"admin_tokens"`
	AppKeys     []string `yaml:"app_keys" json:"app_keys"`
}

type AuthMiddleware struct {
	mu          sync.RWMutex
	adminTokens map[string]struct{}
	appKeys     map[string]struct{}
	enabled     bool
}

func NewAuthMiddleware(cfg *AuthConfig) *AuthMiddleware {
	am := &AuthMiddleware{
		adminTokens: make(map[string]struct{}),
		appKeys:     make(map[string]struct{}),
	}
	if cfg != nil {
		am.UpdateConfig(cfg)
	}
	return am
}

func (am *AuthMiddleware) UpdateConfig(cfg *AuthConfig) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.adminTokens = make(map[string]struct{}, len(cfg.AdminTokens))
	for _, t := range cfg.AdminTokens {
		if t != "" {
			am.adminTokens[t] = struct{}{}
		}
	}

	am.appKeys = make(map[string]struct{}, len(cfg.AppKeys))
	for _, k := range cfg.AppKeys {
		if k != "" {
			am.appKeys[k] = struct{}{}
		}
	}

	am.enabled = len(am.adminTokens) > 0 || len(am.appKeys) > 0
}

func (am *AuthMiddleware) AdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !am.isEnabled() {
			next(w, r)
			return
		}

		token := extractToken(r, "X-Admin-Token", "admin_token")
		if token == "" {
			writeError(w, http.StatusUnauthorized, "missing admin token: set X-Admin-Token header")
			return
		}

		if !am.validateAdminToken(token) {
			writeError(w, http.StatusForbidden, "invalid admin token")
			return
		}

		next(w, r)
	}
}

func (am *AuthMiddleware) ClientAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !am.isEnabled() {
			next(w, r)
			return
		}

		key := extractToken(r, "X-App-Key", "app_key")
		if key == "" {
			writeError(w, http.StatusUnauthorized, "missing app key: set X-App-Key header")
			return
		}

		if !am.validateAppKey(key) {
			writeError(w, http.StatusForbidden, "invalid app key")
			return
		}

		next(w, r)
	}
}

func (am *AuthMiddleware) isEnabled() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.enabled
}

func (am *AuthMiddleware) validateAdminToken(token string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	for t := range am.adminTokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(t)) == 1 {
			return true
		}
	}
	return false
}

func (am *AuthMiddleware) validateAppKey(key string) bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	for k := range am.appKeys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(k)) == 1 {
			return true
		}
	}
	return false
}

func extractToken(r *http.Request, headerName, queryParam string) string {
	if token := r.Header.Get(headerName); token != "" {
		return token
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
	}

	if token := r.URL.Query().Get(queryParam); token != "" {
		return token
	}

	return ""
}
