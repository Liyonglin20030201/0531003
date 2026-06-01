package fsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/linxGnu/grocksdb"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/store"
)

type ConfigEntry struct {
	Key         string
	Value       string
	Environment string
	Namespace   string
	Version     uint64
	CreatedAt   int64
	UpdatedAt   int64
	UpdatedBy   string
	Comment     string
	Deleted     bool
}

type WatchEvent struct {
	Entry     *ConfigEntry
	EventType string
}

type WatcherHub struct {
	mu       sync.RWMutex
	watchers map[string][]chan *WatchEvent
}

func NewWatcherHub() *WatcherHub {
	return &WatcherHub{
		watchers: make(map[string][]chan *WatchEvent),
	}
}

func (h *WatcherHub) Subscribe(env, namespace, key string) (<-chan *WatchEvent, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan *WatchEvent, 64)
	pattern := watchKey(env, namespace, key)
	h.watchers[pattern] = append(h.watchers[pattern], ch)

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		channels := h.watchers[pattern]
		for i, c := range channels {
			if c == ch {
				h.watchers[pattern] = append(channels[:i], channels[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, cancel
}

func (h *WatcherHub) Notify(env, namespace, key string, entry *ConfigEntry, eventType string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	event := &WatchEvent{Entry: entry, EventType: eventType}

	patterns := []string{
		watchKey(env, namespace, key),
		watchKey(env, namespace, ""),
		watchKey("", "", ""),
	}

	for _, pattern := range patterns {
		for _, ch := range h.watchers[pattern] {
			select {
			case ch <- event:
			default:
			}
		}
	}
}

func watchKey(env, namespace, key string) string {
	return env + ":" + namespace + ":" + key
}

type ConfigFSM struct {
	store    *store.RocksDBStore
	watchers *WatcherHub
}

func NewConfigFSM(s *store.RocksDBStore) *ConfigFSM {
	return &ConfigFSM{
		store:    s,
		watchers: NewWatcherHub(),
	}
}

func (f *ConfigFSM) Watchers() *WatcherHub {
	return f.watchers
}

func (f *ConfigFSM) Store() *store.RocksDBStore {
	return f.store
}

func (f *ConfigFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := encoding.Decode(log.Data, &cmd); err != nil {
		return &CommandResponse{Error: err}
	}

	switch cmd.Type {
	case CmdPutConfig:
		return f.applyPutConfig(&cmd)
	case CmdDeleteConfig:
		return f.applyDeleteConfig(&cmd)
	case CmdCreateNamespace:
		return f.applyCreateNamespace(&cmd)
	case CmdDeleteNamespace:
		return f.applyDeleteNamespace(&cmd)
	default:
		return &CommandResponse{Error: ErrUnknownCommand}
	}
}

func (f *ConfigFSM) applyPutConfig(cmd *Command) *CommandResponse {
	baseKey := buildKey(cmd.Environment, cmd.Namespace, cmd.Key)

	currentVersion := f.getVersionCounter(baseKey)

	if cmd.ExpectVersion > 0 && currentVersion != cmd.ExpectVersion {
		return &CommandResponse{
			Error:          ErrVersionConflict,
			CurrentVersion: currentVersion,
		}
	}

	newVersion := currentVersion + 1

	now := time.Now().UnixNano()
	entry := &ConfigEntry{
		Key:         cmd.Key,
		Value:       cmd.Value,
		Environment: cmd.Environment,
		Namespace:   cmd.Namespace,
		Version:     newVersion,
		CreatedAt:   now,
		UpdatedAt:   now,
		UpdatedBy:   cmd.UpdatedBy,
		Comment:     cmd.Comment,
	}

	entryBytes, err := encoding.Encode(entry)
	if err != nil {
		return &CommandResponse{Error: err}
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.PutCF(f.store.CF(store.CFDefault), []byte(baseKey), entryBytes)

	versionKey := buildVersionKey(cmd.Environment, cmd.Namespace, cmd.Key, newVersion)
	batch.PutCF(f.store.CF(store.CFVersions), []byte(versionKey), entryBytes)

	batch.PutCF(f.store.CF(store.CFVersionCounter), []byte(baseKey), uint64ToBytes(newVersion))

	if err := f.store.WriteBatch(batch); err != nil {
		return &CommandResponse{Error: err}
	}

	f.watchers.Notify(cmd.Environment, cmd.Namespace, cmd.Key, entry, "PUT")

	return &CommandResponse{Version: newVersion}
}

func (f *ConfigFSM) applyDeleteConfig(cmd *Command) *CommandResponse {
	baseKey := buildKey(cmd.Environment, cmd.Namespace, cmd.Key)

	currentVersion := f.getVersionCounter(baseKey)

	if cmd.ExpectVersion > 0 && currentVersion != cmd.ExpectVersion {
		return &CommandResponse{
			Error:          ErrVersionConflict,
			CurrentVersion: currentVersion,
		}
	}

	newVersion := currentVersion + 1

	now := time.Now().UnixNano()
	entry := &ConfigEntry{
		Key:         cmd.Key,
		Environment: cmd.Environment,
		Namespace:   cmd.Namespace,
		Version:     newVersion,
		UpdatedAt:   now,
		Comment:     cmd.Comment,
		Deleted:     true,
	}

	entryBytes, err := encoding.Encode(entry)
	if err != nil {
		return &CommandResponse{Error: err}
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.DeleteCF(f.store.CF(store.CFDefault), []byte(baseKey))

	versionKey := buildVersionKey(cmd.Environment, cmd.Namespace, cmd.Key, newVersion)
	batch.PutCF(f.store.CF(store.CFVersions), []byte(versionKey), entryBytes)

	batch.PutCF(f.store.CF(store.CFVersionCounter), []byte(baseKey), uint64ToBytes(newVersion))

	if err := f.store.WriteBatch(batch); err != nil {
		return &CommandResponse{Error: err}
	}

	f.watchers.Notify(cmd.Environment, cmd.Namespace, cmd.Key, entry, "DELETE")

	return &CommandResponse{Version: newVersion}
}

func (f *ConfigFSM) applyCreateNamespace(cmd *Command) *CommandResponse {
	meta := &NamespaceMeta{
		Name:        cmd.Namespace,
		Description: cmd.Comment,
		CreatedAt:   time.Now().UnixNano(),
	}

	data, err := encoding.Encode(meta)
	if err != nil {
		return &CommandResponse{Error: err}
	}

	if err := f.store.PutCF(store.CFNamespaces, []byte(cmd.Namespace), data); err != nil {
		return &CommandResponse{Error: err}
	}

	return &CommandResponse{}
}

func (f *ConfigFSM) applyDeleteNamespace(cmd *Command) *CommandResponse {
	if err := f.store.DeleteCF(store.CFNamespaces, []byte(cmd.Namespace)); err != nil {
		return &CommandResponse{Error: err}
	}

	prefix := ":" + cmd.Namespace + ":"
	f.deleteByPrefix(store.CFDefault, prefix)
	f.deleteByPrefix(store.CFVersions, prefix)
	f.deleteByPrefix(store.CFVersionCounter, prefix)

	return &CommandResponse{}
}

func (f *ConfigFSM) deleteByPrefix(cf string, contains string) {
	it := f.store.NewIteratorCF(cf)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	cfh := f.store.CF(cf)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		keyStr := string(key.Data())
		key.Free()
		if strings.Contains(keyStr, contains) {
			batch.DeleteCF(cfh, []byte(keyStr))
		}
	}

	_ = f.store.WriteBatch(batch)
}

func (f *ConfigFSM) getVersionCounter(baseKey string) uint64 {
	data, err := f.store.GetCF(store.CFVersionCounter, []byte(baseKey))
	if err != nil {
		return 0
	}
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

func (f *ConfigFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &ConfigSnapshot{store: f.store}, nil
}

func (f *ConfigFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return restoreFromSnapshot(f.store, rc)
}

type NamespaceMeta struct {
	Name        string
	Description string
	CreatedAt   int64
}

func buildKey(env, namespace, key string) string {
	return env + ":" + namespace + ":" + key
}

func buildVersionKey(env, namespace, key string, version uint64) string {
	return fmt.Sprintf("%s:%s:%s:%020d", env, namespace, key, version)
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
