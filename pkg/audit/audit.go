package audit

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/YonglinLi/config-center/internal/encoding"
	"github.com/YonglinLi/config-center/pkg/store"
)

type ActionType string

const (
	ActionPutConfig       ActionType = "PUT_CONFIG"
	ActionDeleteConfig    ActionType = "DELETE_CONFIG"
	ActionRollbackConfig  ActionType = "ROLLBACK_CONFIG"
	ActionCreateNamespace ActionType = "CREATE_NAMESPACE"
	ActionDeleteNamespace ActionType = "DELETE_NAMESPACE"
)

type AuditRecord struct {
	ID          uint64     `json:"id"`
	Action      ActionType `json:"action"`
	Environment string     `json:"environment"`
	Namespace   string     `json:"namespace"`
	Key         string     `json:"key"`
	OldValue    string     `json:"old_value,omitempty"`
	NewValue    string     `json:"new_value,omitempty"`
	Version     uint64     `json:"version"`
	Operator    string     `json:"operator"`
	Comment     string     `json:"comment,omitempty"`
	Timestamp   int64      `json:"timestamp"`
}

type AuditStore struct {
	store *store.RocksDBStore
	seq   atomic.Uint64
}

func NewAuditStore(s *store.RocksDBStore) *AuditStore {
	as := &AuditStore{store: s}
	as.seq.Store(as.loadMaxSeq())
	return as
}

func (a *AuditStore) Append(record *AuditRecord) error {
	id := a.seq.Add(1)
	record.ID = id
	if record.Timestamp == 0 {
		record.Timestamp = time.Now().UnixNano()
	}

	data, err := encoding.Encode(record)
	if err != nil {
		return err
	}

	key := uint64ToBytes(id)
	return a.store.PutCF(store.CFAuditLog, key, data)
}

func (a *AuditStore) List(env, namespace, key string, offset, limit int) ([]*AuditRecord, error) {
	it := a.store.NewIteratorCF(store.CFAuditLog)
	defer it.Close()

	if limit <= 0 {
		limit = 50
	}

	var all []*AuditRecord
	for it.SeekToLast(); it.Valid(); it.Prev() {
		value := it.Value()
		var rec AuditRecord
		if err := encoding.Decode(value.Data(), &rec); err != nil {
			value.Free()
			continue
		}
		value.Free()

		if env != "" && rec.Environment != env {
			continue
		}
		if namespace != "" && rec.Namespace != namespace {
			continue
		}
		if key != "" && rec.Key != key {
			continue
		}

		all = append(all, &rec)
	}

	if offset >= len(all) {
		return nil, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

func (a *AuditStore) GetByID(id uint64) (*AuditRecord, error) {
	key := uint64ToBytes(id)
	data, err := a.store.GetCF(store.CFAuditLog, key)
	if err != nil {
		return nil, err
	}
	var rec AuditRecord
	if err := encoding.Decode(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (a *AuditStore) ListByKey(env, namespace, key string, limit int) ([]*AuditRecord, error) {
	return a.List(env, namespace, key, 0, limit)
}

func (a *AuditStore) Summary(env, namespace string) (*AuditSummary, error) {
	it := a.store.NewIteratorCF(store.CFAuditLog)
	defer it.Close()

	summary := &AuditSummary{
		OperatorStats: make(map[string]int),
		ActionStats:   make(map[ActionType]int),
	}

	for it.SeekToFirst(); it.Valid(); it.Next() {
		value := it.Value()
		var rec AuditRecord
		if err := encoding.Decode(value.Data(), &rec); err != nil {
			value.Free()
			continue
		}
		value.Free()

		if env != "" && rec.Environment != env {
			continue
		}
		if namespace != "" && !strings.EqualFold(rec.Namespace, namespace) {
			continue
		}

		summary.TotalChanges++
		summary.OperatorStats[rec.Operator]++
		summary.ActionStats[rec.Action]++
		if rec.Timestamp > summary.LastChangeAt {
			summary.LastChangeAt = rec.Timestamp
		}
	}
	return summary, nil
}

type AuditSummary struct {
	TotalChanges  int                `json:"total_changes"`
	LastChangeAt  int64              `json:"last_change_at"`
	OperatorStats map[string]int     `json:"operator_stats"`
	ActionStats   map[ActionType]int `json:"action_stats"`
}

func (a *AuditStore) loadMaxSeq() uint64 {
	it := a.store.NewIteratorCF(store.CFAuditLog)
	defer it.Close()
	it.SeekToLast()
	if !it.Valid() {
		return 0
	}
	k := it.Key()
	defer k.Free()
	if k.Size() == 8 {
		return binary.BigEndian.Uint64(k.Data())
	}
	return 0
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func FormatAuditComment(action ActionType, targetVersion uint64) string {
	if action == ActionRollbackConfig {
		return fmt.Sprintf("rollback to version %d", targetVersion)
	}
	return ""
}
