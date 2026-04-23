package spdk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
)

const (
	replicaSubDir  = "replicas"
	replicaRecFile = "replica.json"
)

type ReplicaRecord struct {
	Name      string `json:"name"`
	LvsName   string `json:"lvsName"`
	LvsUUID   string `json:"lvsUUID"`
	IP        string `json:"ip"`
	PortStart int32  `json:"portStart"`
	PortEnd   int32  `json:"portEnd"`
}

func replicaRecordDir(metadataDir, replicaName string) string {
	return filepath.Join(metadataDir, replicaSubDir, replicaName)
}

func replicaRecordPath(metadataDir, replicaName string) string {
	return filepath.Join(replicaRecordDir(metadataDir, replicaName), replicaRecFile)
}

func saveReplicaRecord(metadataDir string, r *Replica) error {
	if metadataDir == "" {
		return nil
	}
	dir := replicaRecordDir(metadataDir, r.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("failed to create replica record dir %s: %w", dir, err)
	}
	rec := ReplicaRecord{
		Name:      r.Name,
		LvsName:   r.LvsName,
		LvsUUID:   r.LvsUUID,
		IP:        r.IP,
		PortStart: r.PortStart,
		PortEnd:   r.PortEnd,
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := replicaRecordPath(metadataDir, r.Name) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, replicaRecordPath(metadataDir, r.Name))
}

func removeReplicaRecord(metadataDir, replicaName string) error {
	if metadataDir == "" {
		return nil
	}
	dir := replicaRecordDir(metadataDir, replicaName)
	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func newPortAllocatorWithReservations(portStart, portEnd int32, records map[string]*ReplicaRecord) (*commonbitmap.Bitmap, error) {
	b, err := commonbitmap.NewBitmap(portStart, portEnd)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return b, nil
	}
	type portRange struct{ start, end int32 }
	ranges := make([]portRange, 0, len(records))
	for _, rec := range records {
		if rec.PortStart < portStart || rec.PortEnd > portEnd || rec.PortEnd < rec.PortStart {
			logrus.Warnf("Persisted replica %s port range [%d, %d] outside allocator [%d, %d]; skipping reservation", rec.Name, rec.PortStart, rec.PortEnd, portStart, portEnd)
			continue
		}
		ranges = append(ranges, portRange{rec.PortStart, rec.PortEnd})
	}
	if len(ranges) == 0 {
		return b, nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })

	if _, _, err := b.AllocateRange(portEnd - portStart + 1); err != nil {
		return nil, fmt.Errorf("failed to claim entire port range %d-%d for reservation seeding: %w", portStart, portEnd, err)
	}

	cursor := portStart
	for _, r := range ranges {
		if r.start > cursor {
			if err := b.ReleaseRange(cursor, r.start-1); err != nil {
				return nil, fmt.Errorf("failed to release gap [%d, %d] during reservation seeding: %w", cursor, r.start-1, err)
			}
		}
		if r.end+1 > cursor {
			cursor = r.end + 1
		}
	}
	if cursor <= portEnd {
		if err := b.ReleaseRange(cursor, portEnd); err != nil {
			return nil, fmt.Errorf("failed to release trailing gap [%d, %d] during reservation seeding: %w", cursor, portEnd, err)
		}
	}
	logrus.Infof("Seeded port allocator with %d reserved replica port ranges from persisted records", len(ranges))
	return b, nil
}

func loadReplicaRecords(metadataDir string) (map[string]*ReplicaRecord, error) {
	if metadataDir == "" {
		return nil, nil
	}
	rootDir := filepath.Join(metadataDir, replicaSubDir)
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make(map[string]*ReplicaRecord, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := replicaRecordPath(metadataDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			logrus.WithError(err).Warnf("Failed to read replica record %s", path)
			continue
		}
		var rec ReplicaRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logrus.WithError(err).Warnf("Failed to unmarshal replica record %s", path)
			continue
		}
		out[rec.Name] = &rec
	}
	return out, nil
}
