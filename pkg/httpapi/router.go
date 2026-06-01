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
	mux        *http.ServeMux
}

func NewServer(node *raftnode.RaftNode, auditStore *audit.AuditStore, pollHub *longpoll.Hub) *Server {
	s := &Server{
		node:       node,
		auditStore: auditStore,
		pollHub:    pollHub,
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	// === Admin/Management API ===
	s.mux.HandleFunc("POST /api/admin/namespaces", s.adminCreateNamespace)
	s.mux.HandleFunc("DELETE /api/admin/namespaces/{name}", s.adminDeleteNamespace)
	s.mux.HandleFunc("PUT /api/admin/configs", s.adminPutConfig)
	s.mux.HandleFunc("DELETE /api/admin/configs/{env}/{namespace}/{key}", s.adminDeleteConfig)
	s.mux.HandleFunc("POST /api/admin/configs/rollback", s.adminRollbackConfig)
	s.mux.HandleFunc("GET /api/admin/audit", s.adminListAudit)
	s.mux.HandleFunc("GET /api/admin/audit/{id}", s.adminGetAudit)
	s.mux.HandleFunc("GET /api/admin/audit/summary", s.adminAuditSummary)

	// === Client API ===
	s.mux.HandleFunc("GET /api/client/configs/{env}/{namespace}/{key}", s.clientGetConfig)
	s.mux.HandleFunc("GET /api/client/configs/{env}/{namespace}", s.clientListConfigs)
	s.mux.HandleFunc("GET /api/client/namespaces", s.clientListNamespaces)
	s.mux.HandleFunc("GET /api/client/watch/{env}/{namespace}/{key}", s.clientLongPoll)
	s.mux.HandleFunc("POST /api/client/watch", s.clientLongPollMulti)
}
