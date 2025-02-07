package config

import (
	_ "errors"
	"os"
	"path/filepath"

	"github.com/JoseTorrado/bqtest/pkg/models"
	"gopkg.in/yaml.v3"
)

// TestConfig represents the structure of the YAML test config
type TestConfig struct {
	Tests    []models.Test `yaml:tests`
	BasePath string        `yaml:base_path`
}

func ParseTestConfig(filename string) (*TestConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config TestConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	// Set the base path if not provided
	if config.BasePath == "" {
		config.BasePath = filepath.Dir(filename)
	}

	return &config, nil

	// return nil, errors.New("Error froom insiide the function")
}
