package models

import (
	"errors"
	"path/filepath"
)

// Test represents a single BigQuery test case
type Test struct {
	Name           string `yaml:"name"`
	QueryFile      string `yaml:"query_file"`
	ExpectedOutput string `yaml:"expected_output"`
}

func (t *Test) Validate() error {
	if t.Name == "" {
		return errors.New("Test name cannot be empty")
	}
	if t.QueryFile == "" {
		return errors.New("Query file path cannot be empty")
	}
	if filepath.Ext(t.QueryFile) != ".sql" {
		return errors.New("query file must have .sql extension")
	}
	if t.ExpectedOutput == "" {
		return errors.New("expected output file path cannot be empty")
	}
	if filepath.Ext(t.ExpectedOutput) != ".csv" {
		return errors.New("expected output file must have .csv extension")
	}
	return nil
}
