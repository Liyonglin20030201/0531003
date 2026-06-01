package longpoll

import (
	"context"
	"time"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/fsm"
	"github.com/YonglinLi/config-center/pkg/store"
)

const DefaultTimeout = 30 * time.Second

type ConfigUpdate struct {
	Environment string `json:"environment"`
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	Version     uint64 `json:"version"`
	EventType   string `json:"event_type"`
	UpdatedBy   string `json:"updated_by"`
	UpdatedAt   int64  `json:"updated_at"`
}

type PollResult struct {
	Changed bool          `json:"changed"`
	Update  *ConfigUpdate `json:"update,omitempty"`
	Current *VersionInfo  `json:"current"`
}

type VersionInfo struct {
	Version   uint64 `json:"version"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

type Hub struct {
	watcherHub *fsm.WatcherHub
	store      *store.RocksDBStore
}

func NewHub(watcherHub *fsm.WatcherHub, s *store.RocksDBStore) *Hub {
	return &Hub{
		watcherHub: watcherHub,
		store:      s,
	}
}

func (h *Hub) Subscribe(ctx context.Context, env, namespace, key string, timeout time.Duration) (*PollResult, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch, unsubscribe := h.watcherHub.Subscribe(env, namespace, key)
	defer unsubscribe()

	select {
	case event, ok := <-ch:
		if !ok {
			return h.buildTimeoutResult(env, namespace, key), nil
		}
		update := eventToUpdate(event)
		return &PollResult{
			Changed: true,
			Update:  update,
			Current: &VersionInfo{Version: update.Version, UpdatedAt: update.UpdatedAt},
		}, nil
	case <-ctx.Done():
		return h.buildTimeoutResult(env, namespace, key), nil
	}
}

func (h *Hub) SubscribeMulti(ctx context.Context, keys []WatchKey, timeout time.Duration) (*MultiPollResult, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	merged := make(chan *ConfigUpdate, len(keys))
	var cancels []func()

	for _, k := range keys {
		ch, unsub := h.watcherHub.Subscribe(k.Environment, k.Namespace, k.Key)
		cancels = append(cancels, unsub)
		go func(c <-chan *fsm.WatchEvent) {
			select {
			case event, ok := <-c:
				if ok {
					merged <- eventToUpdate(event)
				}
			case <-ctx.Done():
			}
		}(ch)
	}

	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	select {
	case update := <-merged:
		return &MultiPollResult{
			Changed: true,
			Updates: []*ConfigUpdate{update},
			Versions: h.buildMultiVersions(keys),
		}, nil
	case <-ctx.Done():
		return &MultiPollResult{
			Changed:  false,
			Versions: h.buildMultiVersions(keys),
		}, nil
	}
}

type MultiPollResult struct {
	Changed  bool                    `json:"changed"`
	Updates  []*ConfigUpdate         `json:"updates,omitempty"`
	Versions map[string]*VersionInfo `json:"versions"`
}

type WatchKey struct {
	Environment string `json:"environment"`
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
}

func (h *Hub) buildTimeoutResult(env, namespace, key string) *PollResult {
	vi := h.queryCurrentVersion(env, namespace, key)
	return &PollResult{
		Changed: false,
		Current: vi,
	}
}

func (h *Hub) buildMultiVersions(keys []WatchKey) map[string]*VersionInfo {
	versions := make(map[string]*VersionInfo, len(keys))
	for _, k := range keys {
		compositeKey := k.Environment + ":" + k.Namespace + ":" + k.Key
		versions[compositeKey] = h.queryCurrentVersion(k.Environment, k.Namespace, k.Key)
	}
	return versions
}

func (h *Hub) queryCurrentVersion(env, namespace, key string) *VersionInfo {
	baseKey := env + ":" + namespace + ":" + key
	data, err := h.store.GetCF(store.CFDefault, []byte(baseKey))
	if err != nil {
		return &VersionInfo{Version: 0}
	}
	var entry fsm.ConfigEntry
	if err := encoding.Decode(data, &entry); err != nil {
		return &VersionInfo{Version: 0}
	}
	return &VersionInfo{Version: entry.Version, UpdatedAt: entry.UpdatedAt}
}

func eventToUpdate(event *fsm.WatchEvent) *ConfigUpdate {
	if event == nil || event.Entry == nil {
		return nil
	}
	return &ConfigUpdate{
		Environment: event.Entry.Environment,
		Namespace:   event.Entry.Namespace,
		Key:         event.Entry.Key,
		Value:       event.Entry.Value,
		Version:     event.Entry.Version,
		EventType:   event.EventType,
		UpdatedBy:   event.Entry.UpdatedBy,
		UpdatedAt:   event.Entry.UpdatedAt,
	}
}
