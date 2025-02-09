package runner

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/JoseTorrado/bqtest/pkg/models"
)

func TestRunTest(t *testing.T) {
	runner, err := NewTestRunner()
	if err != nil {
		t.Fatalf("Failed to create TestRunner: %v", err)
	}
	defer runner.Close()

	// Set up dataset
	ctx := context.Background()
	dataset := runner.Client.Dataset("test_dataset")
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
		t.Fatalf("Failed to create dataset: %v", err)
	}

	// Setup test data
	setupQueries := []string{
		"CREATE OR REPLACE TABLE test_dataset.test_table (id INT64, name STRING)",
		"INSERT INTO test_dataset.test_table (id, name) VALUES (1, 'foo'), (2, 'bar')",
	}
	err = runner.SetupTestData(setupQueries)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Create a temp SQL file for the test
	tmpFile, err := os.CreateTemp("", "test*.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	sqlContent := "SELECT * FROM test_dataset.test_table ORDER BY id"
	if _, err := tmpFile.Write([]byte(sqlContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a mock Test
	test := &models.Test{
		Name:      "Mock Test",
		QueryFile: tmpFile.Name(),
	}

	// Run the test
	results, err := runner.RunTest(test)
	if err != nil {
		t.Fatalf("RunTest failed: %v", err)
	}

	// Check the results
	expected := [][]string{
		{"1", "foo"},
		{"2", "bar"},
	}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), len(results))
	}

	for i, row := range results {
		if len(row) != len(expected[i]) {
			t.Fatalf("Row %d: expected %d columns, got %d", i, len(expected[i]), len(row))
		}
		for j, val := range row {
			if val != expected[i][j] {
				t.Errorf("Row %d, Column %d: expected '%s', got '%s'", i, j, expected[i][j], val)
			}
		}
	}
}

func TestCompareResults(t *testing.T) {
	runner := &TestRunner{}

	tests := []struct {
		name      string
		actual    [][]string
		expected  [][]string
		match     bool
		diffCount int
	}{
		{
			name:      "Exact match",
			actual:    [][]string{{"1", "2"}, {"3", "4"}},
			expected:  [][]string{{"1", "2"}, {"3", "4"}},
			match:     true,
			diffCount: 0,
		},
		{
			name:      "Different values",
			actual:    [][]string{{"1", "2"}, {"3", "5"}},
			expected:  [][]string{{"1", "2"}, {"3", "4"}},
			match:     false,
			diffCount: 1,
		},
		{
			name:      "Different row count",
			actual:    [][]string{{"1", "2"}},
			expected:  [][]string{{"1", "2"}, {"3", "4"}},
			match:     false,
			diffCount: 1,
		},
		{
			name:      "Different column count",
			actual:    [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			expected:  [][]string{{"1", "2"}, {"4", "5"}},
			match:     false,
			diffCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, differences := runner.CompareResults(tt.actual, tt.expected)
			if match != tt.match {
				t.Errorf("Expected match to be %v, got %v", tt.match, match)
			}
			if len(differences) != tt.diffCount {
				t.Errorf("Expected %d differences, got %d", tt.diffCount, len(differences))
			}
		})
	}
}
