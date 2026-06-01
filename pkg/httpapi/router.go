package httpapi

import (
	"net/http"

	"github.com/YonglinLi/config-center/pkg/audit"
	"github.com/YonglinLi/config-center/pkg/longpoll"
	"github.com/YonglinLi/config-center/pkg/raftnode"
)

type Server struct {
	node       *raftnode.RaftNode
	auditStore *audit.AuditStore
	pollHub    *longpoll.Hub
	auth       *AuthMiddleware
	mux        *http.ServeMux
}

func NewServer(node *raftnode.RaftNode, auditStore *audit.AuditStore, pollHub *longpoll.Hub, authCfg *AuthConfig) *Server {
	s := &Server{
		node:       node,
		auditStore: auditStore,
		pollHub:    pollHub,
		auth:       NewAuthMiddleware(authCfg),
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	// === Admin/Management API (requires admin token) ===
	s.mux.HandleFunc("POST /api/admin/namespaces", s.auth.AdminAuth(s.adminCreateNamespace))
	s.mux.HandleFunc("DELETE /api/admin/namespaces/{name}", s.auth.AdminAuth(s.adminDeleteNamespace))
	s.mux.HandleFunc("PUT /api/admin/configs", s.auth.AdminAuth(s.adminPutConfig))
	s.mux.HandleFunc("DELETE /api/admin/configs/{env}/{namespace}/{key}", s.auth.AdminAuth(s.adminDeleteConfig))
	s.mux.HandleFunc("POST /api/admin/configs/rollback", s.auth.AdminAuth(s.adminRollbackConfig))
	s.mux.HandleFunc("GET /api/admin/audit", s.auth.AdminAuth(s.adminListAudit))
	s.mux.HandleFunc("GET /api/admin/audit/{id}", s.auth.AdminAuth(s.adminGetAudit))
	s.mux.HandleFunc("GET /api/admin/audit/summary", s.auth.AdminAuth(s.adminAuditSummary))

	// === Client API (requires app key) ===
	s.mux.HandleFunc("GET /api/client/configs/{env}/{namespace}/{key}", s.auth.ClientAuth(s.clientGetConfig))
	s.mux.HandleFunc("GET /api/client/configs/{env}/{namespace}", s.auth.ClientAuth(s.clientListConfigs))
	s.mux.HandleFunc("GET /api/client/namespaces", s.auth.ClientAuth(s.clientListNamespaces))
	s.mux.HandleFunc("GET /api/client/watch/{env}/{namespace}/{key}", s.auth.ClientAuth(s.clientLongPoll))
	s.mux.HandleFunc("POST /api/client/watch", s.auth.ClientAuth(s.clientLongPollMulti))
}
