package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type RocksetConfig struct {
	ApiKey    string `yaml:"api_key"`
	ApiServer string `yaml:"api_server"`
}

type S3Config struct {
	Integration string `yaml:"integration"`
	Uri         string `yaml:"uri"`
}

type MongoConfig struct {
	Uri         string `yaml:"uri"`
	Integration string `yaml:"integration"`
	DB          string `yaml:"db"`
	Collection  string `yaml:"collection"`

	TargetChunkSizeMB uint64 `yaml:"target_file_size_mb"`
}

type Config struct {
	Rockset RocksetConfig `yaml:"rockset"`

	S3 S3Config `yaml:"s3"`

	Mongo MongoConfig `yaml:"mongo"`

	RocksetCollection       string                 `yaml:"rockset_collection"`
	CreateCollectionRequest map[string]interface{} `yaml:"create_collection_request"`
	LoadOnly                bool                   `yaml:"load_only"`
}

func ReadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var c Config
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &c, nil
}
