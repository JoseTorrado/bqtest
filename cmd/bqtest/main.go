package main

import (
	"fmt"
	"log"
	"os"

	"github.com/JoseTorrado/bqtest/pkg/config"
	"github.com/JoseTorrado/bqtest/pkg/runner"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "bqtest",
		Usage: "A CLI tool for running BigQuery tests",
		Commands: []*cli.Command{
			{
				Name:    "run",
				Aliases: []string{"r"},
				Usage:   "Run BigQuery tests",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "config",
						Aliases:  []string{"c"},
						Usage:    "Path to the test configuration file",
						Required: true,
					},
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "Enable verbose output",
					},
				},
				Action: runTests,
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "List available tests",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "config",
						Aliases:  []string{"c"},
						Usage:    "Path to the test configuration file",
						Required: true,
					},
				},
				Action: listTests,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runTests(c *cli.Context) error {
	configFile := c.String("config")
	verbose := c.Bool("verbose")

	// Parse the test configuration
	testConfig, err := config.ParseTestConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to parse test configuration: %v", err)
	}

	// Validate the test configuration
	if err := testConfig.Validate(); err != nil {
		return fmt.Errorf("invalid test configuration: %v", err)
	}

	// Create a new test runner
	testRunner, err := runner.NewTestRunner()
	if err != nil {
		return fmt.Errorf("failed to create test runner: %v", err)
	}
	defer testRunner.Close()

	// Run tests
	for _, test := range testConfig.Tests {
		fmt.Printf("Running test: %s\n", test.Name)

		// Run the test query
		actualResults, err := testRunner.RunTest(&test)
		if err != nil {
			fmt.Printf("Error running test '%s': %v\n", test.Name, err)
			continue
		}

		// Get expected results
		expectedResults, err := test.GetExpectedOutput()
		if err != nil {
			fmt.Printf("Error getting expected output for test '%s': %v\n", test.Name, err)
			continue
		}

		// Compare results
		passed, differences := testRunner.CompareResults(actualResults, expectedResults)

		if passed {
			fmt.Printf("Test '%s' passed!\n", test.Name)
		} else {
			fmt.Printf("Test '%s' failed. Differences:\n", test.Name)
			for _, diff := range differences {
				fmt.Println(diff)
			}
		}

		if verbose {
			fmt.Printf("Actual results:\n%v\n", actualResults)
			fmt.Printf("Expected results:\n%v\n", expectedResults)
		}

		fmt.Println()
	}

	return nil
}

func listTests(c *cli.Context) error {
	configFile := c.String("config")

	// Parse the test configuration
	testConfig, err := config.ParseTestConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to parse test configuration: %v", err)
	}

	fmt.Println("Available tests:")
	for _, test := range testConfig.Tests {
		fmt.Printf("- %s\n", test.Name)
	}

	return nil
}
