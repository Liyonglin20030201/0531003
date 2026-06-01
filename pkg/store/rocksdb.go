package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
)

const (
	CFDefault        = "default"
	CFVersions       = "versions"
	CFVersionCounter = "version_counter"
	CFNamespaces     = "namespaces"
	CFRaftLog        = "raft_log"
	CFRaftStable     = "raft_stable"
)

var allColumnFamilies = []string{
	CFDefault,
	CFVersions,
	CFVersionCounter,
	CFNamespaces,
	CFRaftLog,
	CFRaftStable,
}

var ErrKeyNotFound = errors.New("key not found")

type RocksDBStore struct {
	db      *grocksdb.DB
	cfhs    map[string]*grocksdb.ColumnFamilyHandle
	dataDir string
	ro      *grocksdb.ReadOptions
	wo      *grocksdb.WriteOptions
}

func NewRocksDBStore(dataDir string) (*RocksDBStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(256 * 1024 * 1024))
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfOpts := make([]*grocksdb.Options, len(allColumnFamilies))
	for i := range allColumnFamilies {
		cfOpts[i] = grocksdb.NewDefaultOptions()
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, dataDir, allColumnFamilies, cfOpts)
	if err != nil {
		return nil, err
	}

	cfhs := make(map[string]*grocksdb.ColumnFamilyHandle, len(allColumnFamilies))
	for i, name := range allColumnFamilies {
		cfhs[name] = cfHandles[i]
	}

	return &RocksDBStore{
		db:      db,
		cfhs:    cfhs,
		dataDir: dataDir,
		ro:      grocksdb.NewDefaultReadOptions(),
		wo:      grocksdb.NewDefaultWriteOptions(),
	}, nil
}

func (s *RocksDBStore) CF(name string) *grocksdb.ColumnFamilyHandle {
	return s.cfhs[name]
}

func (s *RocksDBStore) DataDir() string {
	return s.dataDir
}

func (s *RocksDBStore) GetCF(cf string, key []byte) ([]byte, error) {
	handle, ok := s.cfhs[cf]
	if !ok {
		return nil, errors.New("unknown column family: " + cf)
	}

	slice, err := s.db.GetCF(s.ro, handle, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, ErrKeyNotFound
	}

	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	return data, nil
}

func (s *RocksDBStore) PutCF(cf string, key, value []byte) error {
	handle, ok := s.cfhs[cf]
	if !ok {
		return errors.New("unknown column family: " + cf)
	}
	return s.db.PutCF(s.wo, handle, key, value)
}

func (s *RocksDBStore) DeleteCF(cf string, key []byte) error {
	handle, ok := s.cfhs[cf]
	if !ok {
		return errors.New("unknown column family: " + cf)
	}
	return s.db.DeleteCF(s.wo, handle, key)
}

func (s *RocksDBStore) WriteBatch(batch *grocksdb.WriteBatch) error {
	return s.db.Write(s.wo, batch)
}

func (s *RocksDBStore) NewIteratorCF(cf string) *grocksdb.Iterator {
	handle := s.cfhs[cf]
	return s.db.NewIteratorCF(s.ro, handle)
}

func (s *RocksDBStore) Checkpoint(dir string) error {
	if err := os.MkdirAll(filepath.Dir(dir), 0755); err != nil {
		return err
	}
	checkpoint, err := s.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer checkpoint.Destroy()
	return checkpoint.CreateCheckpoint(dir, 0)
}

func (s *RocksDBStore) Close() {
	s.ro.Destroy()
	s.wo.Destroy()
	for _, cfh := range s.cfhs {
		cfh.Destroy()
	}
	s.db.Close()
}

// ReplaceFromDir closes the current DB, removes the data directory,
// moves newDataDir into its place, and reopens the DB. This is used
// during snapshot restore to atomically swap in a new dataset.
func (s *RocksDBStore) ReplaceFromDir(newDataDir string) error {
	// 1. Close current DB and release all handles
	s.ro.Destroy()
	s.wo.Destroy()
	for _, cfh := range s.cfhs {
		cfh.Destroy()
	}
	s.db.Close()

	// 2. Remove existing data directory
	if err := os.RemoveAll(s.dataDir); err != nil {
		return fmt.Errorf("remove old data dir: %w", err)
	}

	// 3. Move snapshot data into the data directory path
	if err := os.Rename(newDataDir, s.dataDir); err != nil {
		return fmt.Errorf("move snapshot to data dir: %w", err)
	}

	// 4. Reopen DB with all column families
	if err := s.reopen(); err != nil {
		return fmt.Errorf("reopen db after restore: %w", err)
	}

	return nil
}

func (s *RocksDBStore) reopen() error {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(256 * 1024 * 1024))
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfOpts := make([]*grocksdb.Options, len(allColumnFamilies))
	for i := range allColumnFamilies {
		cfOpts[i] = grocksdb.NewDefaultOptions()
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, s.dataDir, allColumnFamilies, cfOpts)
	if err != nil {
		return err
	}

	cfhs := make(map[string]*grocksdb.ColumnFamilyHandle, len(allColumnFamilies))
	for i, name := range allColumnFamilies {
		cfhs[name] = cfHandles[i]
	}

	s.db = db
	s.cfhs = cfhs
	s.ro = grocksdb.NewDefaultReadOptions()
	s.wo = grocksdb.NewDefaultWriteOptions()
	return nil
}
