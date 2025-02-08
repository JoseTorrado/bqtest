package fileutil

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
)

func ReadSQLFile(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	// Trim whitespace and end with ';'
	query := strings.TrimSpace(string(content))
	if !strings.HasSuffix(query, ";") {
		query += ";"
	}

	return query, nil

}

func ReadSQLFiles(filenames ...string) (map[string]string, error) {
	queries := make(map[string]string)

	for _, filename := range filenames {
		query, err := ReadSQLFile(filename)
		if err != nil {
			return nil, err
		}

		queries[filepath.Base(filename)] = query // Not really sure about this
	}

	return queries, nil
}

func ReadCSVFile(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}
