package fsm

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
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
	// Extract snapshot tar to a temporary directory next to the data dir
	restoreDir := filepath.Join(filepath.Dir(s.DataDir()), "restore-tmp")
	os.RemoveAll(restoreDir)
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		log.Printf("[FSM Restore] failed to create restore temp dir %s: %v", restoreDir, err)
		return fmt.Errorf("create restore dir: %w", err)
	}

	log.Printf("[FSM Restore] extracting snapshot to temp dir: %s", restoreDir)

	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[FSM Restore] failed to read tar entry: %v", err)
			os.RemoveAll(restoreDir)
			return fmt.Errorf("read tar header: %w", err)
		}

		target := filepath.Join(restoreDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				log.Printf("[FSM Restore] failed to create directory %s: %v", target, err)
				os.RemoveAll(restoreDir)
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				log.Printf("[FSM Restore] failed to create parent dir for %s: %v", target, err)
				os.RemoveAll(restoreDir)
				return err
			}
			file, err := os.Create(target)
			if err != nil {
				log.Printf("[FSM Restore] failed to create file %s: %v", target, err)
				os.RemoveAll(restoreDir)
				return err
			}
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				log.Printf("[FSM Restore] failed to write file %s: %v", target, err)
				os.RemoveAll(restoreDir)
				return err
			}
			file.Close()
		}
	}

	log.Printf("[FSM Restore] snapshot extracted, replacing database at %s", s.DataDir())

	// Close current DB, replace data directory with snapshot, reopen
	if err := s.ReplaceFromDir(restoreDir); err != nil {
		log.Printf("[FSM Restore] FATAL: database replacement failed: %v", err)
		os.RemoveAll(restoreDir)
		return fmt.Errorf("replace db from snapshot: %w", err)
	}

	log.Printf("[FSM Restore] restore completed successfully")
	return nil
}
