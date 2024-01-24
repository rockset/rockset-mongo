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

func (c *Config) Validate() error {
	if c.RocksetCollection == "" {
		return fmt.Errorf("missing rockset `collection`")
	}

	// rockset fields
	if c.Rockset.ApiKey == "" {
		return fmt.Errorf("missing rockset.api_key")
	}
	if c.Rockset.ApiServer == "" {
		return fmt.Errorf("missing rockset.api_server")
	}

	// S3 fields
	if c.S3.Uri == "" {
		return fmt.Errorf("missing s3.uri")
	}
	if c.S3.Integration == "" {
		return fmt.Errorf("missing s3.integration")
	}

	// Mongo
	if c.Mongo.Uri == "" {
		return fmt.Errorf("missing mongo.uri")
	}
	if !c.LoadOnly && c.Mongo.Integration == "" {
		return fmt.Errorf("missing mongo.integration")
	}
	if c.Mongo.DB == "" {
		return fmt.Errorf("missing mongo.db")
	}
	if c.Mongo.Collection == "" {
		return fmt.Errorf("missing mongo.collection")
	}

	return nil
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
