package runner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
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

// ValueSaverRow implements the ValueSaver interface
type ValueSaverRow struct {
	Row []bigquery.Value
}

func NewTestRunner() (*TestRunner, error) {
	ctx := context.Background()

	// Start the bigquery emulator
	srv, err := server.New(server.TempStorage)
	if err != nil {
		return nil, fmt.Errorf("Failed to create BigQuery emulator: %v", err)
	}

	srv.SetLogLevel("error")

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

	// Ensure the dataset exists
	if err := r.ensureDatasetExists(ctx); err != nil {
		return err
	}

	// Read the CSV file
	records, err := fileutil.ReadCSVFile(test.InputFile)
	if err != nil {
		return fmt.Errorf("failed to read input CSV: %v", err)
	}

	if len(records) < 2 {
		return fmt.Errorf("CSV file must contain at least a header row and one data row")
	}

	headers := records[0]

	// Create schema based on the CSV headers and overrides
	schema := bigquery.Schema{}
	for _, header := range headers {
		fieldType := bigquery.StringFieldType // Default to string
		if override, ok := test.SchemaOverrides[header]; ok {
			fieldType = getBigQueryFieldType(override)
		}
		schema = append(schema, &bigquery.FieldSchema{
			Name: formatFieldName(header),
			Type: fieldType,
		})
	}

	// Create the table
	tableRef := r.Client.Dataset(testDatasetID).Table(test.TableName)
	if err := tableRef.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Prepare the data for insertion
	var rows []map[string]bigquery.Value
	for _, record := range records[1:] { // Skip the header row
		row := make(map[string]bigquery.Value)
		for i, value := range record {
			if i < len(headers) {
				convertedValue, err := convertValue(value, schema[i].Type)
				if err != nil {
					return fmt.Errorf("failed to convert value: %v", err)
				}
				row[formatFieldName(headers[i])] = convertedValue
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

func formatFieldName(s string) string {
	// Capitalize the first letter and remove any non-alphanumeric characters
	r := []rune(s)
	return string(append([]rune{unicode.ToUpper(r[0])}, r[1:]...))
}

func getBigQueryFieldType(typeString string) bigquery.FieldType {
	switch strings.ToUpper(typeString) {
	case "INTEGER":
		return bigquery.IntegerFieldType
	case "FLOAT":
		return bigquery.FloatFieldType
	case "BOOLEAN":
		return bigquery.BooleanFieldType
	case "TIMESTAMP":
		return bigquery.TimestampFieldType
	case "DATE":
		return bigquery.DateFieldType
	default:
		return bigquery.StringFieldType
	}
}

func convertValue(value string, fieldType bigquery.FieldType) (bigquery.Value, error) {
	switch fieldType {
	case bigquery.IntegerFieldType:
		return strconv.ParseInt(value, 10, 64)
	case bigquery.FloatFieldType:
		return strconv.ParseFloat(value, 64)
	case bigquery.BooleanFieldType:
		return strconv.ParseBool(value)
	case bigquery.TimestampFieldType:
		return time.Parse(time.RFC3339, value)
	case bigquery.DateFieldType:
		return civil.ParseDate(value)
	default:
		return value, nil
	}
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
