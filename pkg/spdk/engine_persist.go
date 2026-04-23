package spdk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	engineSubDir  = "engines"
	engineRecFile = "engine.json"
)

// EngineRecord holds the metadata needed to reconstruct an Engine in-memory
// after an instance-manager / SPDK restart, without re-running the full
// replica-add / rebuild flow. It is persisted to
// <metadataDir>/engines/<engineName>/engine.json.
//
// The record deliberately excludes transient state (rebuild phase, dirty
// bitmaps, Head/SnapshotMap). Those are re-derived from the live SPDK bdev
// inventory during Engine.ValidateAndUpdate. What matters for recovery is the
// mapping from logical replica name to {bdev name, address, mode} so the RAID
// base-bdev relationship can be re-established against the existing SPDK
// objects that survived the restart.
type EngineRecord struct {
	Name                string                          `json:"name"`
	VolumeName          string                          `json:"volumeName"`
	Frontend            string                          `json:"frontend"`
	SpecSize            uint64                          `json:"specSize"`
	RaidBdevUUID        string                          `json:"raidBdevUUID,omitempty"`
	ReplicaTransport    NvmfTransportType               `json:"replicaTransport,omitempty"`
	ReplicaStatusMap    map[string]*EngineReplicaStatus `json:"replicaStatusMap,omitempty"`
	NvmeTcpTarget       *NvmeTcpTarget                  `json:"nvmeTcpTarget,omitempty"`
	DeltaBitmapEnabled  bool                            `json:"deltaBitmapEnabled,omitempty"`
	ReplicaDirtyBitmaps map[string]*ReplicaDirtyBitmap  `json:"replicaDirtyBitmaps,omitempty"`
}

func engineRecordDir(metadataDir, engineName string) string {
	return filepath.Join(metadataDir, engineSubDir, engineName)
}

func engineRecordPath(metadataDir, engineName string) string {
	return filepath.Join(engineRecordDir(metadataDir, engineName), engineRecFile)
}

// saveEngineRecord persists the engine metadata to disk atomically.
// Callers must hold the Engine lock (this function reads the Engine fields
// directly to avoid per-field copies; a consistent snapshot matters because
// ReplicaStatusMap is mutated concurrently with replica-add / raid updates).
func saveEngineRecord(metadataDir string, e *Engine) error {
	if metadataDir == "" {
		return nil
	}

	replicaStatusCopy := make(map[string]*EngineReplicaStatus, len(e.ReplicaStatusMap))
	for name, status := range e.ReplicaStatusMap {
		if status == nil {
			continue
		}
		replicaStatusCopy[name] = &EngineReplicaStatus{
			Address:       status.Address,
			DialedAddress: status.DialedAddress,
			BdevName:      status.BdevName,
			Mode:          status.Mode,
			Transport:     status.Transport,
		}
	}

	var nvmeTarget *NvmeTcpTarget
	if e.NvmeTcpTarget != nil {
		t := *e.NvmeTcpTarget
		nvmeTarget = &t
	}

	var bitmapsCopy map[string]*ReplicaDirtyBitmap
	if len(e.ReplicaDirtyBitmaps) > 0 {
		bitmapsCopy = make(map[string]*ReplicaDirtyBitmap, len(e.ReplicaDirtyBitmaps))
		for name, bm := range e.ReplicaDirtyBitmaps {
			if bm == nil {
				continue
			}
			b := *bm
			bitmapsCopy[name] = &b
		}
	}

	rec := EngineRecord{
		Name:                e.Name,
		VolumeName:          e.VolumeName,
		Frontend:            e.Frontend,
		SpecSize:            e.SpecSize,
		RaidBdevUUID:        e.RaidBdevUUID,
		ReplicaTransport:    e.ReplicaTransport,
		ReplicaStatusMap:    replicaStatusCopy,
		NvmeTcpTarget:       nvmeTarget,
		DeltaBitmapEnabled:  e.deltaBitmapEnabled,
		ReplicaDirtyBitmaps: bitmapsCopy,
	}

	dir := engineRecordDir(metadataDir, e.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("failed to create engine record dir %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal engine record for %s: %w", e.Name, err)
	}

	targetPath := engineRecordPath(metadataDir, e.Name)
	tmpPath := targetPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o640); err != nil {
		return fmt.Errorf("failed to write engine record temp file %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			logrus.WithError(rmErr).Warnf("Failed to remove engine record temp file %s", tmpPath)
		}
		return fmt.Errorf("failed to rename engine record %s -> %s: %w", tmpPath, targetPath, err)
	}
	return nil
}

func removeEngineRecord(metadataDir, engineName string) error {
	if metadataDir == "" {
		return nil
	}
	dir := engineRecordDir(metadataDir, engineName)
	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove engine record directory %s: %w", dir, err)
	}
	return nil
}

// loadEngineRecords scans the engine records directory and returns all valid
// records keyed by engine name. Invalid or corrupted records are logged and
// removed so they don't block recovery on the next restart.
func loadEngineRecords(metadataDir string) (map[string]*EngineRecord, error) {
	if metadataDir == "" {
		return nil, nil
	}

	baseDir := filepath.Join(metadataDir, engineSubDir)

	var entries []os.DirEntry
	var readErr error
	for attempt := 0; attempt < 3; attempt++ {
		entries, readErr = os.ReadDir(baseDir)
		if readErr == nil {
			break
		}
		if os.IsNotExist(readErr) {
			return nil, nil
		}
		logrus.WithError(readErr).Warnf("Failed to read engine records directory %s (attempt %d/3)", baseDir, attempt+1)
		time.Sleep(500 * time.Millisecond)
	}
	if readErr != nil {
		return nil, fmt.Errorf("failed to read engine records directory %s after retries: %w", baseDir, readErr)
	}

	out := map[string]*EngineRecord{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		engineName := entry.Name()
		recordPath := engineRecordPath(metadataDir, engineName)

		data, err := os.ReadFile(recordPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			logrus.WithError(err).Warnf("Failed to read engine record %s, skipping", recordPath)
			continue
		}

		rec := &EngineRecord{}
		if err := json.Unmarshal(data, rec); err != nil {
			logrus.WithError(err).Warnf("Failed to parse engine record %s, removing corrupted record", recordPath)
			if rmErr := os.RemoveAll(filepath.Join(baseDir, engineName)); rmErr != nil {
				logrus.WithError(rmErr).Warnf("Failed to remove corrupted engine record directory %s", engineName)
			}
			continue
		}

		if rec.Name == "" || rec.VolumeName == "" {
			logrus.Warnf("Engine record %s has empty name or volume name, removing invalid record", recordPath)
			if rmErr := os.RemoveAll(filepath.Join(baseDir, engineName)); rmErr != nil {
				logrus.WithError(rmErr).Warnf("Failed to remove invalid engine record directory %s", engineName)
			}
			continue
		}

		out[rec.Name] = rec
	}

	return out, nil
}

// restoreFromRecord rebuilds an Engine's in-memory state from a persisted
// record. The Engine must be freshly constructed via NewEngine; callers are
// responsible for wiring it into the server's engineMap and driving a
// subsequent ValidateAndUpdate to reconcile against live SPDK bdev state.
func (e *Engine) restoreFromRecord(rec *EngineRecord) {
	if rec == nil {
		return
	}
	e.Name = rec.Name
	e.VolumeName = rec.VolumeName
	e.Frontend = rec.Frontend
	e.SpecSize = rec.SpecSize
	e.RaidBdevUUID = rec.RaidBdevUUID
	e.ReplicaTransport = rec.ReplicaTransport
	if rec.ReplicaStatusMap != nil {
		e.ReplicaStatusMap = make(map[string]*EngineReplicaStatus, len(rec.ReplicaStatusMap))
		for name, s := range rec.ReplicaStatusMap {
			if s == nil {
				continue
			}
			e.ReplicaStatusMap[name] = &EngineReplicaStatus{
				Address:  s.Address,
				BdevName: s.BdevName,
				Mode:     s.Mode,
			}
		}
	}
	if rec.NvmeTcpTarget != nil {
		t := *rec.NvmeTcpTarget
		e.NvmeTcpTarget = &t
	}
	e.deltaBitmapEnabled = rec.DeltaBitmapEnabled
	if len(rec.ReplicaDirtyBitmaps) > 0 {
		e.ReplicaDirtyBitmaps = make(map[string]*ReplicaDirtyBitmap, len(rec.ReplicaDirtyBitmaps))
		for name, bm := range rec.ReplicaDirtyBitmaps {
			if bm == nil {
				continue
			}
			b := *bm
			e.ReplicaDirtyBitmaps[name] = &b
		}
	}
	e.log.Infof("Restored engine %s (volume=%s) from persisted record: replicas=%d raid=%s deltaBitmap=%v bitmaps=%d",
		e.Name, e.VolumeName, len(e.ReplicaStatusMap), e.RaidBdevUUID, e.deltaBitmapEnabled, len(e.ReplicaDirtyBitmaps))
}

