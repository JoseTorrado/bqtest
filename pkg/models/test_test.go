package models

import "testing"

func TestTest(t *testing.T) {
	testCase := Test{
		Name:           "Simple SELECT Test",
		Query:          "SELECT 1 as num",
		ExpectedOutput: "1",
	}

	if testCase.Name != "Simple SELECT Test" {
		t.Errorf("Expected test name to be 'Simple SELECT Test', got '%s'", testCase.Name)
	}

	if testCase.Query != "SELECT 1 as num" {
		t.Errorf("Expected query to be 'SELECT 1 as num', got '%s'", testCase.Query)
	}

	if testCase.ExpectedOutput != "1" {
		t.Errorf("Expected output to be '1', got '%s'", testCase.ExpectedOutput)
	}
}

func TestValidate(t *testing.T) {
	t.Run("Valid Test", func(t *testing.T) {
		testCase := Test{
			Name:           "Valid Test",
			Query:          "SELECT 1",
			ExpectedOutput: "1",
		}

		if err := testCase.Validate(); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("Invalid Test - No Name", func(t *testing.T) {
		testCase := Test{
			Query:          "SELECT 1",
			ExpectedOutput: "1",
		}
		if err := testCase.Validate(); err == nil {
			t.Error("Expected an error due to missing name, got none")
		}
	})

	t.Run("Invalid Test - No Query", func(t *testing.T) {
		testCase := Test{
			Name:           "Invalid Test",
			ExpectedOutput: "1",
		}
		if err := testCase.Validate(); err == nil {
			t.Error("Expected an error due to missing query, got none")
		}
	})

}
