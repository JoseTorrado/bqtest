package models

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTest(t *testing.T) {
	t.Run("Valid Test", func(t *testing.T) {
		test := Test{
			Name:           "Valid Test",
			QueryFile:      "query.sql",
			ExpectedOutput: "output.csv",
		}
		if err := test.Validate(); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("Invalid Test - No Name", func(t *testing.T) {
		test := Test{
			QueryFile:      "query.sql",
			ExpectedOutput: "output.csv",
		}
		if err := test.Validate(); err == nil {
			t.Error("Expected an error due to missing name, got none")
		}
	})

	t.Run("Invalid Test - No Query File", func(t *testing.T) {
		test := Test{
			Name:           "Invalid Test",
			ExpectedOutput: "output.csv",
		}
		if err := test.Validate(); err == nil {
			t.Error("Expected an error due to missing query file, got none")
		}
	})

	t.Run("Invalid Test - Wrong Query File Extension", func(t *testing.T) {
		test := Test{
			Name:           "Invalid Test",
			QueryFile:      "query.txt",
			ExpectedOutput: "output.csv",
		}
		if err := test.Validate(); err == nil {
			t.Error("Expected an error due to wrong query file extension, got none")
		}
	})

	t.Run("Invalid Test - No Expected Output", func(t *testing.T) {
		test := Test{
			Name:      "Invalid Test",
			QueryFile: "query.sql",
		}
		if err := test.Validate(); err == nil {
			t.Error("Expected an error due to missing expected output, got none")
		}
	})

	t.Run("Invalid Test - Wrong Expected Output Extension", func(t *testing.T) {
		test := Test{
			Name:           "Invalid Test",
			QueryFile:      "query.sql",
			ExpectedOutput: "output.txt",
		}
		if err := test.Validate(); err == nil {
			t.Error("Expected an error due to wrong expected output extension, got none")
		}
	})
}

func TestGetQuery(t *testing.T) {
	// Temp directory for our files
	tmpDir, err := os.MkdirTemp("", "testquery")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SQL test file
	testSQL := "SELECT * FROM table WHERE id = 1"
	sqlFilePath := filepath.Join(tmpDir, "test.sql")
	err = os.WriteFile(sqlFilePath, []byte(testSQL), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Creating a test instance
	test := Test{
		Name:           "Test Query",
		QueryFile:      sqlFilePath,
		ExpectedOutput: "dummy.csv",
	}

	query, err := test.GetQuery()
	if err != nil {
		t.Fatalf("Failed to get query: %v", err)
	}

	expectedQuery := testSQL + ";"
	if query != expectedQuery {
		t.Errorf("Expected query %q, got %q", expectedQuery, query)
	}

	// Test caching
	// Modify the file content
	err = os.WriteFile(sqlFilePath, []byte("SELECT 1;"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	cachedQuery, err := test.GetQuery()
	if err != nil {
		t.Fatalf("Failed toget cahched query: %v", err)
	}

	if cachedQuery != expectedQuery {
		t.Errorf("Expected cached query %q, got %q", expectedQuery, cachedQuery)
	}
}

func TestGetExpectedOutput(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "testexpectedoutput")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test CSV file
	testCSV := "column1,column2,column3\nvalue1,value2,value3\nvalue4,value5,value6"
	csvFilePath := filepath.Join(tmpDir, "expected.csv")
	err = os.WriteFile(csvFilePath, []byte(testCSV), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Create a test instance
	test := Test{
		Name:           "Test Expected Output",
		QueryFile:      "dummy.sql",
		ExpectedOutput: csvFilePath,
	}

	// Test GetExpectedOutput method
	output, err := test.GetExpectedOutput()
	if err != nil {
		t.Fatalf("Failed to get expected output: %v", err)
	}

	expectedOutput := [][]string{
		{"column1", "column2", "column3"},
		{"value1", "value2", "value3"},
		{"value4", "value5", "value6"},
	}

	if !reflect.DeepEqual(output, expectedOutput) {
		t.Errorf("Expected output %v, got %v", expectedOutput, output)
	}

	// Test caching
	// Modify the file content
	err = os.WriteFile(csvFilePath, []byte("new,csv,content"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// GetExpectedOutput should return the cached version
	cachedOutput, err := test.GetExpectedOutput()
	if err != nil {
		t.Fatalf("Failed to get cached expected output: %v", err)
	}

	if !reflect.DeepEqual(cachedOutput, expectedOutput) {
		t.Errorf("Expected cached output %v, got %v", expectedOutput, cachedOutput)
	}
}
