package models

import (
	"errors"
	"path/filepath"

	"github.com/JoseTorrado/bqtest/pkg/fileutil"
)

// Test represents a single BigQuery test case
type Test struct {
	Name           string `yaml:"name"`
	QueryFile      string `yaml:"query_file"`
	ExpectedOutput string `yaml:"expected_output"`
	query          string
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

func (t *Test) GetQuery() (string, error) {
	if t.query == "" {
		var err error
		t.query, err = fileutil.ReadSQLFile(t.QueryFile)
		if err != nil {
			return "", err
		}
	}
	return t.query, nil
}
