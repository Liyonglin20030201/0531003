package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/audit"
	"github.com/YonglinLi/config-center/pkg/fsm"
	"github.com/YonglinLi/config-center/pkg/store"
)

// --- Namespace Management ---

type CreateNamespaceReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (s *Server) adminCreateNamespace(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}

	var req CreateNamespaceReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	cmd := &fsm.Command{
		Type:      fsm.CmdCreateNamespace,
		Namespace: req.Name,
		Comment:   req.Description,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if resp.Error != nil {
		writeError(w, http.StatusInternalServerError, resp.Error.Error())
		return
	}

	s.auditStore.Append(&audit.AuditRecord{
		Action:    audit.ActionCreateNamespace,
		Namespace: req.Name,
		Operator:  r.Header.Get("X-Operator"),
		Comment:   req.Description,
	})

	writeJSON(w, http.StatusCreated, map[string]interface{}{"success": true})
}

func (s *Server) adminDeleteNamespace(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}

	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	cmd := &fsm.Command{
		Type:      fsm.CmdDeleteNamespace,
		Namespace: name,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if resp.Error != nil {
		writeError(w, http.StatusInternalServerError, resp.Error.Error())
		return
	}

	s.auditStore.Append(&audit.AuditRecord{
		Action:    audit.ActionDeleteNamespace,
		Namespace: name,
		Operator:  r.Header.Get("X-Operator"),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// --- Config CRUD ---

type PutConfigReq struct {
	Environment   string `json:"environment"`
	Namespace     string `json:"namespace"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	Operator      string `json:"operator"`
	Comment       string `json:"comment"`
	ExpectVersion uint64 `json:"expect_version,omitempty"`
}

func (s *Server) adminPutConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}

	var req PutConfigReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Environment == "" || req.Namespace == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "environment, namespace, and key are required")
		return
	}

	oldValue := s.getCurrentValue(req.Environment, req.Namespace, req.Key)

	cmd := &fsm.Command{
		Type:          fsm.CmdPutConfig,
		Namespace:     req.Namespace,
		Environment:   req.Environment,
		Key:           req.Key,
		Value:         req.Value,
		UpdatedBy:     req.Operator,
		Comment:       req.Comment,
		ExpectVersion: req.ExpectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":           "version conflict",
				"current_version": resp.CurrentVersion,
				"expect_version":  req.ExpectVersion,
				"message":         "the config has been modified since you last read it; re-fetch and retry",
			})
			return
		}
		writeError(w, http.StatusInternalServerError, resp.Error.Error())
		return
	}

	s.auditStore.Append(&audit.AuditRecord{
		Action:      audit.ActionPutConfig,
		Environment: req.Environment,
		Namespace:   req.Namespace,
		Key:         req.Key,
		OldValue:    oldValue,
		NewValue:    req.Value,
		Version:     resp.Version,
		Operator:    req.Operator,
		Comment:     req.Comment,
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"version": resp.Version,
	})
}

func (s *Server) adminDeleteConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}

	env := r.PathValue("env")
	namespace := r.PathValue("namespace")
	key := r.PathValue("key")

	operator := r.Header.Get("X-Operator")
	comment := r.URL.Query().Get("comment")
	expectVersion := parseUint64(r.URL.Query().Get("expect_version"))

	oldValue := s.getCurrentValue(env, namespace, key)

	cmd := &fsm.Command{
		Type:          fsm.CmdDeleteConfig,
		Namespace:     namespace,
		Environment:   env,
		Key:           key,
		Comment:       comment,
		ExpectVersion: expectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":           "version conflict",
				"current_version": resp.CurrentVersion,
				"expect_version":  expectVersion,
				"message":         "the config has been modified since you last read it; re-fetch and retry",
			})
			return
		}
		writeError(w, http.StatusInternalServerError, resp.Error.Error())
		return
	}

	s.auditStore.Append(&audit.AuditRecord{
		Action:      audit.ActionDeleteConfig,
		Environment: env,
		Namespace:   namespace,
		Key:         key,
		OldValue:    oldValue,
		Version:     resp.Version,
		Operator:    operator,
		Comment:     comment,
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

// --- Rollback ---

type RollbackReq struct {
	Environment   string `json:"environment"`
	Namespace     string `json:"namespace"`
	Key           string `json:"key"`
	TargetVersion uint64 `json:"target_version"`
	ExpectVersion uint64 `json:"expect_version,omitempty"`
	Operator      string `json:"operator"`
}

func (s *Server) adminRollbackConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}

	var req RollbackReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Environment == "" || req.Namespace == "" || req.Key == "" || req.TargetVersion == 0 {
		writeError(w, http.StatusBadRequest, "environment, namespace, key, and target_version are required")
		return
	}

	versionKey := fmt.Sprintf("%s:%s:%s:%020d", req.Environment, req.Namespace, req.Key, req.TargetVersion)
	data, err := s.node.Store.GetCF(store.CFVersions, []byte(versionKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			writeError(w, http.StatusNotFound, "target version not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	oldValue := s.getCurrentValue(req.Environment, req.Namespace, req.Key)

	cmd := &fsm.Command{
		Type:          fsm.CmdPutConfig,
		Namespace:     req.Namespace,
		Environment:   req.Environment,
		Key:           req.Key,
		Value:         entry.Value,
		UpdatedBy:     req.Operator,
		Comment:       fmt.Sprintf("rollback to version %d", req.TargetVersion),
		ExpectVersion: req.ExpectVersion,
	}

	resp, err := s.node.Apply(cmd, 5*time.Second)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if resp.Error != nil {
		if resp.Error == fsm.ErrVersionConflict {
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error":           "version conflict",
				"current_version": resp.CurrentVersion,
				"expect_version":  req.ExpectVersion,
				"message":         "the config has been modified since you last read it; re-fetch current version before rollback",
			})
			return
		}
		writeError(w, http.StatusInternalServerError, resp.Error.Error())
		return
	}

	s.auditStore.Append(&audit.AuditRecord{
		Action:      audit.ActionRollbackConfig,
		Environment: req.Environment,
		Namespace:   req.Namespace,
		Key:         req.Key,
		OldValue:    oldValue,
		NewValue:    entry.Value,
		Version:     resp.Version,
		Operator:    req.Operator,
		Comment:     fmt.Sprintf("rollback to version %d", req.TargetVersion),
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"new_version": resp.Version,
	})
}

// --- Audit Queries ---

func (s *Server) adminListAudit(w http.ResponseWriter, r *http.Request) {
	env := r.URL.Query().Get("environment")
	namespace := r.URL.Query().Get("namespace")
	key := r.URL.Query().Get("key")
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	records, err := s.auditStore.List(env, namespace, key, offset, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"records": records,
		"count":   len(records),
	})
}

func (s *Server) adminGetAudit(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid audit id")
		return
	}

	record, err := s.auditStore.GetByID(id)
	if err != nil {
		if err == store.ErrKeyNotFound {
			writeError(w, http.StatusNotFound, "audit record not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, record)
}

func (s *Server) adminAuditSummary(w http.ResponseWriter, r *http.Request) {
	env := r.URL.Query().Get("environment")
	namespace := r.URL.Query().Get("namespace")

	summary, err := s.auditStore.Summary(env, namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

// --- Helpers ---

func (s *Server) getCurrentValue(env, namespace, key string) string {
	baseKey := env + ":" + namespace + ":" + key
	data, err := s.node.Store.GetCF(store.CFDefault, []byte(baseKey))
	if err != nil {
		return ""
	}
	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return ""
	}
	return entry.Value
}

func (s *Server) requireLeader(w http.ResponseWriter) bool {
	if s.node.IsLeader() {
		return true
	}
	leaderAddr := s.node.LeaderAddress()
	if leaderAddr == "" {
		writeError(w, http.StatusServiceUnavailable, "no leader elected")
		return false
	}
	w.Header().Set("X-Leader-Address", leaderAddr)
	writeError(w, http.StatusTemporaryRedirect, "not leader, redirect to: "+leaderAddr)
	return false
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseUint64(s string) uint64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
