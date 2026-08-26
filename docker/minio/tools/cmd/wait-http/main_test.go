package main

import (
	"strings"
	"testing"
)

func TestRunRequiresReadinessURL(t *testing.T) {
	err := run(func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "READY_URL") {
		t.Fatalf("expected missing URL error, got %v", err)
	}
}
