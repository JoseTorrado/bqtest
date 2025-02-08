package fileutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadSQLFile(t *testing.T) {
	// Temp directory for our files
	tmpDir, err := os.MkdirTemp("", "sqltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create SQL test file
	testSQL := "SELECT * FROM table WHERE id = 1"
	sqlFilePath := filepath.Join(tmpDir, "test.sql")
	if err = os.WriteFile(sqlFilePath, []byte(testSQL), 0644); err != nil {
		t.Fatal(err)
	}

	// Test Reading the SQL file
	content, err := ReadSQLFile(sqlFilePath)
	if err != nil {
		t.Fatalf("Failed to read SQL file: %v", err)
	}

	expectedContent := testSQL + ";"
	if content != expectedContent {
		t.Errorf("Expected content %q, got %q", expectedContent, content)
	}

}

func TestReadSQLFiles(t *testing.T) {
	// Temp directory for our files
	tmpDir, err := os.MkdirTemp("", "sqltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test SQL files
	testFiles := map[string]string{
		"test1.sql": "SELECT * FROM table1;",
		"test2.sql": "SELECT * from table 2 WHERE id = 2",
	}

	var filePaths []string
	for filename, content := range testFiles {
		filePath := filepath.Join(tmpDir, filename)
		err = os.WriteFile(filePath, []byte(content), 0644)
		if err != nil {
			t.Fatal(err)
		}
		filePaths = append(filePaths, filePath)
	}

	// Test reading multiple files
	queries, err := ReadSQLFiles(filePaths...)
	if err != nil {
		t.Fatalf("Failed to read SQL files: %v", err)
	}

	if len(queries) != len(testFiles) {
		t.Errorf("Expected %d queries, got %d", len(testFiles), len(queries))
	}

}
