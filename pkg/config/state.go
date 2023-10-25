package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

type State struct {
	ID             uuid.UUID       `json:"id"`
	ExportInfo     *ExportInfo     `json:"export"`
	CollectionInfo *CollectionInfo `json:"collection_info"`
}

type ExportInfo struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"stop_time"`

	Bucket string `json:"s3_bucket"`
	Prefix string `json:"s3_prefix"`
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
		return NewState(), nil
	}

	var s State
	err = json.Unmarshal(data, &s)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &s, nil
}