package longpoll

import (
	"context"
	"time"

	"github.com/YonglinLi/config-center/pkg/fsm"
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

type Hub struct {
	watcherHub *fsm.WatcherHub
}

func NewHub(watcherHub *fsm.WatcherHub) *Hub {
	return &Hub{
		watcherHub: watcherHub,
	}
}

func (h *Hub) Subscribe(ctx context.Context, env, namespace, key string, timeout time.Duration) (*ConfigUpdate, error) {
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
			return nil, nil
		}
		return eventToUpdate(event), nil
	case <-ctx.Done():
		return nil, nil
	}
}

func (h *Hub) SubscribeMulti(ctx context.Context, keys []WatchKey, timeout time.Duration) ([]*ConfigUpdate, error) {
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
		return []*ConfigUpdate{update}, nil
	case <-ctx.Done():
		return nil, nil
	}
}

type WatchKey struct {
	Environment string `json:"environment"`
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
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
