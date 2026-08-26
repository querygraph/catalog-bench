package infra

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func TestWaitReadyRetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := WaitReady(ctx, server.Client(), server.URL, time.Millisecond); err != nil {
		t.Fatalf("wait for readiness: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected three readiness calls, got %d", calls.Load())
	}
}

func TestCheckReadyBoundsResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(strings.Repeat("x", maxInfraResponseBytes+1)))
	}))
	defer server.Close()

	err := CheckReady(context.Background(), server.Client(), server.URL)
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("expected bounded response failure, got %v", err)
	}
}
