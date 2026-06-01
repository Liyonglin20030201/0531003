package fsm

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"

	"github.com/YonglinLi/config-center/pkg/store"
)

type ConfigSnapshot struct {
	store *store.RocksDBStore
}

func (s *ConfigSnapshot) Persist(sink raft.SnapshotSink) error {
	checkpointDir := filepath.Join(s.store.DataDir(), "snapshots",
		fmt.Sprintf("snap-%d", time.Now().UnixNano()))

	if err := s.store.Checkpoint(checkpointDir); err != nil {
		sink.Cancel()
		return err
	}
	defer os.RemoveAll(checkpointDir)

	tw := tar.NewWriter(sink)
	defer tw.Close()

	err := filepath.Walk(checkpointDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(checkpointDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(tw, file)
		return err
	})

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (s *ConfigSnapshot) Release() {}

func restoreFromSnapshot(s *store.RocksDBStore, rc io.Reader) error {
	restoreDir := filepath.Join(s.DataDir(), "restore-tmp")
	os.RemoveAll(restoreDir)
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(restoreDir)

	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(restoreDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			file, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				return err
			}
			file.Close()
		}
	}

	return nil
}
