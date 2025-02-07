package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JoseTorrado/bqtest/pkg/models"
)

func TestParseTestConfig(t *testing.T) {
	// Temp dir for our test files
	tmpDir, err := os.MkdirTemp("", "bqtest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Dummy SQL and CSVs
	if err := os.WriteFile(filepath.Join(tmpDir, "query1.sql"), []byte("SELECT * FROM table1"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "expected1.csv"), []byte("column1,column2\nvalue1,value2"), 0644); err != nil {
		t.Fatal(err)
	}

	yamlContent := `
base_path: %s
tests:
  - name: "Test 1"
    query_file: "query1.sql"
    expected_output: "expected1.csv"
`

	yamlContent = fmt.Sprintf(yamlContent, tmpDir)

	tmpFile, err := os.CreateTemp(tmpDir, "test*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(yamlContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Test parsing
	config, err := ParseTestConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to parse test config: %v", err)
	}

	// Check if the parsed config matches our expectations
	if len(config.Tests) != 1 {
		t.Errorf("Expected 1 test, got %d", len(config.Tests))
	}

	expectedTest := models.Test{
		Name:           "Test 1",
		QueryFile:      "query1.sql",
		ExpectedOutput: "expected1.csv",
	}

	if config.Tests[0] != expectedTest {
		t.Errorf("Test does not match. Got %+v, want %+v", config.Tests[0], expectedTest)
	}

	// Validate the config
	// if err := config.Validate(); err != nil {
	// 	t.Errorf("Config validation failed: %v", err)
	// }
}
