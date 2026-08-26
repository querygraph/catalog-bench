package infra

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckReadyAcceptsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := CheckReady(context.Background(), server.Client(), server.URL); err != nil {
		t.Fatalf("check ready: %v", err)
	}
}

func TestCheckReadyRejectsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := CheckReady(context.Background(), server.Client(), server.URL)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected status-bearing failure, got %v", err)
	}
}
