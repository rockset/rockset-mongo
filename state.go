package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

type State struct {
	ID             uuid.UUID       `json:"id"`
	CollectionInfo *CollectionInfo `json:"collection_info"`
	ExportInfo     *ExportInfo     `json:"export"`
}

type ExportInfo struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"stop_time"`
}

type CollectionInfo struct {
	ResumeToken string `json:"resume_token"`
	Documents   uint64 `json:"documents"`
	Size        uint64 `json:"storage"`
}

func NewState() *State {
	return &State{
		ID: uuid.New(),
	}
}

func (s *State) ToJson() string {
	d, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		panic(fmt.Errorf("struct is serializable: %v", err))
	}

	return string(d)
}

func (s *State) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(s.ToJson())
	return err
}

func ReadState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var s State
	err = json.Unmarshal(data, &s)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &s, nil
}

func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	// all supported numerical types (except for complex)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)

	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)

	case float32:
		return uint64(v)
	case float64:
		return uint64(v)

	default:
		panic(fmt.Errorf("unsupported type: %T", value))
	}
}
