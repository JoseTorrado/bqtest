package runner

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/JoseTorrado/bqtest/pkg/models"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type TestRunner struct {
	Client *bigquery.Client
	server *server.Server
}

func NewTestRunner() (*TestRunner, error) {
	ctx := context.Background()

	// Start the bigquery emulator
	srv, err := server.New(server.TempStorage)
	if err != nil {
		return nil, fmt.Errorf("Failed to create BigQuery emulator: %v", err)
	}

	// Create a test project
	if err := srv.Load(server.StructSource(types.NewProject("test-project"))); err != nil {
		return nil, fmt.Errorf("Failed to create test project: %v", err)
	}

	// Create a BigQuery client that connects to the emulator
	client, err := bigquery.NewClient(
		ctx,
		"test-project",
		option.WithEndpoint(srv.TestServer().URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to create BigQuery CLient: %v", err)
	}

	return &TestRunner{
		Client: client,
		server: srv,
	}, nil
}

// I am still shaky on this function... Need to look over it
func (r *TestRunner) RunTest(test *models.Test) ([][]string, error) {
	ctx := context.Background()

	query, err := test.GetQuery()
	if err != nil {
		return nil, fmt.Errorf("Failed to get query: %v", err)
	}

	q := r.Client.Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed tu run query: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("faield to wait for job: %v", err)
	}

	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("job failed: %v", err)
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("falied to read job results: %v", err)
	}

	var results [][]string
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Failed to iterate over results: %v", err)
		}

		stringRow := make([]string, len(row))
		for i, v := range row {
			stringRow[i] = fmt.Sprintf("%v", v)
		}
		results = append(results, stringRow)
	}

	return results, nil
}

// also shaky on this one
// CompareResults compares the actual results with the expected output
func (r *TestRunner) CompareResults(actual, expected [][]string) (bool, []string) {
	if len(actual) != len(expected) {
		return false, []string{fmt.Sprintf("Row count mismatch: expected %d, got %d", len(expected), len(actual))}
	}

	var differences []string
	for i := range actual {
		if len(actual[i]) != len(expected[i]) {
			differences = append(differences, fmt.Sprintf("Row %d: Column count mismatch: expected %d, got %d", i, len(expected[i]), len(actual[i])))
			continue
		}
		for j := range actual[i] {
			if actual[i][j] != expected[i][j] {
				differences = append(differences, fmt.Sprintf("Row %d, Column %d: expected '%s', got '%s'", i, j, expected[i][j], actual[i][j]))
			}
		}
	}

	return len(differences) == 0, differences
}

// Close closes the BigQuery client and stops the emulator
func (r *TestRunner) Close() error {
	if err := r.Client.Close(); err != nil {
		return err
	}
	return r.server.Close()
}

// SetupTestData sets up any necessary test data in the emulator
func (r *TestRunner) SetupTestData(setupQueries []string) error {
	ctx := context.Background()
	for _, query := range setupQueries {
		q := r.Client.Query(query)
		job, err := q.Run(ctx)
		if err != nil {
			return fmt.Errorf("failed to run setup query: %v", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return fmt.Errorf("failed to wait for setup job: %v", err)
		}
		if err := status.Err(); err != nil {
			return fmt.Errorf("setup job failed: %v", err)
		}
	}
	return nil
}
