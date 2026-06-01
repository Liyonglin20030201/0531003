package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/fsm"
	"github.com/YonglinLi/config-center/pkg/longpoll"
	"github.com/YonglinLi/config-center/pkg/store"
)

type ConfigItemResp struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	Environment string `json:"environment"`
	Namespace   string `json:"namespace"`
	Version     uint64 `json:"version"`
	UpdatedBy   string `json:"updated_by,omitempty"`
	Comment     string `json:"comment,omitempty"`
	UpdatedAt   int64  `json:"updated_at,omitempty"`
}

func (s *Server) clientGetConfig(w http.ResponseWriter, r *http.Request) {
	env := r.PathValue("env")
	namespace := r.PathValue("namespace")
	key := r.PathValue("key")

	versionStr := r.URL.Query().Get("version")
	if versionStr != "" {
		version, err := strconv.ParseUint(versionStr, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid version")
			return
		}
		s.getConfigByVersion(w, env, namespace, key, version)
		return
	}

	baseKey := env + ":" + namespace + ":" + key
	data, err := s.node.Store.GetCF(store.CFDefault, []byte(baseKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			writeError(w, http.StatusNotFound, "config not found")
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

	writeJSON(w, http.StatusOK, entryToResp(&entry))
}

func (s *Server) getConfigByVersion(w http.ResponseWriter, env, namespace, key string, version uint64) {
	versionKey := env + ":" + namespace + ":" + key + ":" + padVersion(version)
	data, err := s.node.Store.GetCF(store.CFVersions, []byte(versionKey))
	if err != nil {
		if err == store.ErrKeyNotFound {
			writeError(w, http.StatusNotFound, "version not found")
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

	writeJSON(w, http.StatusOK, entryToResp(&entry))
}

func (s *Server) clientListConfigs(w http.ResponseWriter, r *http.Request) {
	env := r.PathValue("env")
	namespace := r.PathValue("namespace")
	prefix := r.URL.Query().Get("prefix")
	limitStr := r.URL.Query().Get("limit")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	searchPrefix := env + ":" + namespace + ":" + prefix
	it := s.node.Store.NewIteratorCF(store.CFDefault)
	defer it.Close()

	var items []*ConfigItemResp
	for it.Seek([]byte(searchPrefix)); it.Valid() && len(items) < limit; it.Next() {
		k := it.Key()
		keyStr := string(k.Data())
		k.Free()

		if !strings.HasPrefix(keyStr, searchPrefix) {
			break
		}

		value := it.Value()
		var entry fsm.ConfigEntry
		if err := encoding.Decode(value.Data(), &entry); err != nil {
			value.Free()
			continue
		}
		value.Free()

		items = append(items, entryToResp(&entry))
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": items,
		"count": len(items),
	})
}

func (s *Server) clientListNamespaces(w http.ResponseWriter, r *http.Request) {
	it := s.node.Store.NewIteratorCF(store.CFNamespaces)
	defer it.Close()

	var namespaces []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		namespaces = append(namespaces, string(k.Data()))
		k.Free()
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespaces": namespaces,
	})
}

// --- Long Polling ---

func (s *Server) clientLongPoll(w http.ResponseWriter, r *http.Request) {
	env := r.PathValue("env")
	namespace := r.PathValue("namespace")
	key := r.PathValue("key")

	timeout := parseDuration(r.URL.Query().Get("timeout"), longpoll.DefaultTimeout)

	update, err := s.pollHub.Subscribe(r.Context(), env, namespace, key, timeout)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if update == nil {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	writeJSON(w, http.StatusOK, update)
}

type LongPollMultiReq struct {
	Keys    []longpoll.WatchKey `json:"keys"`
	Timeout string              `json:"timeout"`
}

func (s *Server) clientLongPollMulti(w http.ResponseWriter, r *http.Request) {
	var req LongPollMultiReq
	if !decodeJSON(w, r, &req) {
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "keys is required")
		return
	}

	timeout := parseDuration(req.Timeout, longpoll.DefaultTimeout)

	updates, err := s.pollHub.SubscribeMulti(r.Context(), req.Keys, timeout)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(updates) == 0 {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"updates": updates,
	})
}

// --- Helpers ---

func entryToResp(entry *fsm.ConfigEntry) *ConfigItemResp {
	return &ConfigItemResp{
		Key:         entry.Key,
		Value:       entry.Value,
		Environment: entry.Environment,
		Namespace:   entry.Namespace,
		Version:     entry.Version,
		UpdatedBy:   entry.UpdatedBy,
		Comment:     entry.Comment,
		UpdatedAt:   entry.UpdatedAt,
	}
}

func padVersion(v uint64) string {
	s := strconv.FormatUint(v, 10)
	for len(s) < 20 {
		s = "0" + s
	}
	return s
}

func parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	// try as seconds
	if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
		d := time.Duration(secs) * time.Second
		if d > 120*time.Second {
			return 120 * time.Second
		}
		return d
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d > 120*time.Second {
			return 120 * time.Second
		}
		return d
	}
	return def
}
