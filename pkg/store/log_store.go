package store

import (
	"encoding/binary"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/linxGnu/grocksdb"
)

type RaftLogStore struct {
	store *RocksDBStore
}

func NewRaftLogStore(store *RocksDBStore) *RaftLogStore {
	return &RaftLogStore{store: store}
}

func (l *RaftLogStore) FirstIndex() (uint64, error) {
	it := l.store.NewIteratorCF(CFRaftLog)
	defer it.Close()

	it.SeekToFirst()
	if !it.Valid() {
		return 0, nil
	}

	key := it.Key()
	defer key.Free()
	return binary.BigEndian.Uint64(key.Data()), nil
}

func (l *RaftLogStore) LastIndex() (uint64, error) {
	it := l.store.NewIteratorCF(CFRaftLog)
	defer it.Close()

	it.SeekToLast()
	if !it.Valid() {
		return 0, nil
	}

	key := it.Key()
	defer key.Free()
	return binary.BigEndian.Uint64(key.Data()), nil
}

func (l *RaftLogStore) GetLog(index uint64, log *raft.Log) error {
	key := uint64ToBytes(index)
	data, err := l.store.GetCF(CFRaftLog, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return raft.ErrLogNotFound
		}
		return err
	}
	return decodeRaftLog(data, log)
}

func (l *RaftLogStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

func (l *RaftLogStore) StoreLogs(logs []*raft.Log) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	cfh := l.store.CF(CFRaftLog)
	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		data, err := encodeRaftLog(log)
		if err != nil {
			return err
		}
		batch.PutCF(cfh, key, data)
	}

	return l.store.WriteBatch(batch)
}

func (l *RaftLogStore) DeleteRange(min, max uint64) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	cfh := l.store.CF(CFRaftLog)
	batch.DeleteRangeCF(cfh, uint64ToBytes(min), uint64ToBytes(max+1))

	return l.store.WriteBatch(batch)
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func encodeRaftLog(log *raft.Log) ([]byte, error) {
	buf := make([]byte, 0, 64+len(log.Data)+len(log.Extensions))
	buf = binary.BigEndian.AppendUint64(buf, log.Index)
	buf = binary.BigEndian.AppendUint64(buf, log.Term)
	buf = append(buf, byte(log.Type))

	dataLen := uint32(len(log.Data))
	buf = binary.BigEndian.AppendUint32(buf, dataLen)
	buf = append(buf, log.Data...)

	extLen := uint32(len(log.Extensions))
	buf = binary.BigEndian.AppendUint32(buf, extLen)
	buf = append(buf, log.Extensions...)

	buf = binary.BigEndian.AppendUint64(buf, uint64(log.AppendedAt.UnixNano()))
	return buf, nil
}

func decodeRaftLog(data []byte, log *raft.Log) error {
	if len(data) < 25 {
		return errors.New("invalid raft log data")
	}

	log.Index = binary.BigEndian.Uint64(data[0:8])
	log.Term = binary.BigEndian.Uint64(data[8:16])
	log.Type = raft.LogType(data[16])

	offset := 17
	dataLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if dataLen > 0 {
		log.Data = make([]byte, dataLen)
		copy(log.Data, data[offset:offset+int(dataLen)])
		offset += int(dataLen)
	}

	extLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if extLen > 0 {
		log.Extensions = make([]byte, extLen)
		copy(log.Extensions, data[offset:offset+int(extLen)])
		offset += int(extLen)
	}

	if offset+8 <= len(data) {
		nanos := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		_ = nanos
	}

	return nil
}

