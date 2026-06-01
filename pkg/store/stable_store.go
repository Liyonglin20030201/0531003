package store

import (
	"encoding/binary"
	"errors"
)

type RaftStableStore struct {
	store *RocksDBStore
}

func NewRaftStableStore(store *RocksDBStore) *RaftStableStore {
	return &RaftStableStore{store: store}
}

func (s *RaftStableStore) Set(key []byte, val []byte) error {
	return s.store.PutCF(CFRaftStable, key, val)
}

func (s *RaftStableStore) Get(key []byte) ([]byte, error) {
	val, err := s.store.GetCF(CFRaftStable, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *RaftStableStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return s.store.PutCF(CFRaftStable, key, buf)
}

func (s *RaftStableStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.store.GetCF(CFRaftStable, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(val) != 8 {
		return 0, errors.New("invalid uint64 value")
	}
	return binary.BigEndian.Uint64(val), nil
}
