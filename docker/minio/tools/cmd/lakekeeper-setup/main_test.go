package main

import (
	"strings"
	"testing"
)

func TestRunRejectsUnknownOperationBeforeNetworkAccess(t *testing.T) {
	err := run([]string{"unknown", "argument"}, func(name string) string {
		if name == "LAKEKEEPER_URL" {
			return "http://lakekeeper:8181"
		}
		return ""
	})
	if err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("expected an unknown-operation error, got %v", err)
	}
}

func TestRunRejectsInvalidArity(t *testing.T) {
	err := run([]string{"ready"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), usage) {
		t.Fatalf("expected usage error, got %v", err)
	}
}
