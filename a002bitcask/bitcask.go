package a002bitcask

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type MetadataSegment struct {
	Id   int    `json:"id"`
	Path string `json:"path"`
}

type Metadata struct {
	CurrentSegmentId int               `json:"current_segment_id"`
	Segments         []MetadataSegment `json:"segments"`
}

const metadataFileName = "metadata.json"

const rotateSegmentSize = 1024 * 1024 // 1MB

type DB struct {
	dir            string
	segments       []*Segment
	currentSegment *Segment
	metadata       Metadata
	rotateMux      sync.RWMutex
}

func NewDB(dir string) (*DB, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, err
	}

	if err := createMetadataIfNotExist(dir); err != nil {
		return nil, err
	}

	metadata, err := readMetadata(dir)
	if err != nil {
		return nil, err
	}

	segments, currentSegment, err := restoreSegments(metadata, dir)
	if err != nil {
		return nil, err
	}

	return &DB{
		dir:            dir,
		segments:       segments,
		currentSegment: currentSegment,
		metadata:       *metadata,
	}, nil
}

func (db *DB) Put(key, value []byte) error {
	_, err := db.currentSegment.Put(key, value)
	if err != nil {
		return err
	}

	if db.currentSegment.Size() >= rotateSegmentSize {
		if err := db.rotateSegment(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.rotateMux.RLock()
	defer db.rotateMux.RUnlock()

	for i := len(db.segments) - 1; i >= 0; i-- {
		seg := db.segments[i]
		value, err := seg.Get(key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return nil, err
		}

		return value, nil
	}

	return nil, ErrKeyNotFound
}

func (db *DB) Delete(key []byte) error {
	db.rotateMux.Lock()
	defer db.rotateMux.Unlock()

	for i := len(db.segments) - 1; i >= 0; i-- {
		seg := db.segments[i]
		if err := seg.Delete(key); err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				continue
			}
			return err
		}

		return nil
	}

	return ErrKeyNotFound
}

func (db *DB) CompactAndMerge() error {
	panic("CompactAndMerge not implemented")
}

func (db *DB) rotateSegment() error {
	db.rotateMux.Lock()
	defer db.rotateMux.Unlock()

	newSegmentId := db.metadata.CurrentSegmentId + 1
	segmentPath := segmentFileName(db.dir, newSegmentId)

	segment, err := NewSegment(segmentPath)
	if err != nil {
		return err
	}

	db.segments = append(db.segments, segment)
	db.currentSegment = segment

	newMetadata := &Metadata{
		CurrentSegmentId: newSegmentId,
		Segments: append(db.metadata.Segments, MetadataSegment{
			Id:   newSegmentId,
			Path: segmentPath,
		}),
	}
	if err := newMetadata.writeToFile(db.dir); err != nil {
		return err
	}

	metadata, err := readMetadata(db.dir)
	if err != nil {
		return err
	}
	db.metadata = *metadata
	return nil
}

func createMetadataIfNotExist(dir string) error {
	metaPath := filepath.Join(dir, metadataFileName)
	if _, err := os.Stat(metaPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		const id = 1
		initMeta := Metadata{
			CurrentSegmentId: id,
			Segments: []MetadataSegment{
				{
					Id:   id,
					Path: segmentFileName(dir, id),
				},
			},
		}

		if err := initMeta.writeToFile(dir); err != nil {
			return err
		}
	}

	return nil
}

func segmentFileName(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%d.data", id))
}

func readMetadata(dir string) (*Metadata, error) {
	var metadata Metadata
	{
		metaPath := filepath.Join(dir, metadataFileName)
		metaBytes, err := os.ReadFile(metaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	return &metadata, nil
}

func restoreSegments(metadata *Metadata, dir string) (
	segments []*Segment, currentSegment *Segment, err error) {

	segments = make([]*Segment, len(metadata.Segments))
	for i, meta := range metadata.Segments {
		segment, err := NewSegment(segmentFileName(dir, meta.Id))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create segment: %w", err)
		}
		segments[i] = segment
		if meta.Id == metadata.CurrentSegmentId {
			currentSegment = segment
		}
	}

	if currentSegment == nil {
		return nil, nil, fmt.Errorf("current segment not found")
	}

	return segments, currentSegment, nil
}

func (m *Metadata) writeToFile(dir string) error {
	metaBytes, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, metadataFileName), metaBytes, 0644); err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}
	return nil
}
