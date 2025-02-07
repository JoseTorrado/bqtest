package main

import "testing"

func TestMain(t *testing.T) {
	t.Run("FirstTest", func(t *testing.T) {
		if false {
			t.Error("This test should always pass!")
		}
	})
}
