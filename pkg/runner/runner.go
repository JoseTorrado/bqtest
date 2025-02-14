package runner

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/JoseTorrado/bqtest/pkg/fileutil"
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

const (
	testDatasetID = "test_dataset"
)

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

// Add this new method to TestRunner
func (r *TestRunner) ensureDatasetExists(ctx context.Context) error {
	dataset := r.Client.Dataset(testDatasetID)
	meta, err := dataset.Metadata(ctx)
	if err != nil {
		// If the dataset doesn't exist, create it
		if err := dataset.Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
			return fmt.Errorf("failed to create dataset: %v", err)
		}
	} else if meta != nil {
		// Dataset already exists
		return nil
	}
	return nil
}

func (r *TestRunner) LoadTestData(test *models.Test) error {
	ctx := context.Background()

	// Vlaidate datset exists
	if err := r.ensureDatasetExists(ctx); err != nil {
		return err
	}

	// Read the CSV file
	records, err := fileutil.ReadCSVFile(test.InputFile)
	if err != nil {
		return fmt.Errorf("Failed to read input CSV: %v", err)
	}

	// Create a schema based on the first row of the CSV
	var schema bigquery.Schema
	headers := records[0]
	for _, header := range headers {
		schema = append(schema, &bigquery.FieldSchema{Name: header, Type: bigquery.StringFieldType})
	}

	// Create the table
	tableRef := r.Client.Dataset(testDatasetID).Table(test.TableName)
	if err := tableRef.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Prepare the data for insertion
	var rows [][]bigquery.Value
	for _, record := range records[1:] { // Skip the header row
		row := make([]bigquery.Value, len(headers))
		for i, value := range record {
			if i < len(headers) {
				row[i] = value
			}
		}
		rows = append(rows, row)
	}

	// Insert the data
	inserter := tableRef.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		return fmt.Errorf("failed to insert data: %v", err)
	}

	return nil
}

// I am still shaky on this function... Need to look over it
func (r *TestRunner) RunTest(test *models.Test) ([][]string, error) {
	ctx := context.Background()
	// Load the test data
	if err := r.LoadTestData(test); err != nil {
		return nil, fmt.Errorf("failed to load test data: %v", err)
	}

	query, err := test.GetQuery()
	if err != nil {
		return nil, fmt.Errorf("failed to get query: %v", err)
	}

	// Replace table name in query if necessary
	query = strings.ReplaceAll(query, "${TABLE}", fmt.Sprintf("`%s.%s`", testDatasetID, test.TableName))

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
