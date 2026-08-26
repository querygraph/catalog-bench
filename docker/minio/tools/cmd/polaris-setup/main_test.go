package main

import (
	"strings"
	"testing"
)

func TestRunRejectsUnknownOperationBeforeNetworkAccess(t *testing.T) {
	err := run([]string{"unknown"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), usage) {
		t.Fatalf("expected usage error, got %v", err)
	}
}

func TestRunRejectsInvalidArity(t *testing.T) {
	err := run(nil, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), usage) {
		t.Fatalf("expected usage error, got %v", err)
	}
}
