package infra

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestJSONHTTPClientSendsTypedRequestAndDecodesBoundedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/items" {
			t.Errorf("unexpected request target: %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("warehouse") != "bench" {
			t.Errorf("unexpected query: %v", request.URL.Query())
		}
		if request.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("unexpected authorization header")
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ready":true}`))
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test URL: %v", err)
	}
	client := NewJSONHTTPClient(server.Client(), baseURL, "fixture")
	var output struct {
		Ready bool `json:"ready"`
	}
	err = client.Do(
		context.Background(),
		http.MethodPost,
		"/api/items",
		url.Values{"warehouse": []string{"bench"}},
		[]byte(`{"name":"fixture"}`),
		http.Header{"Authorization": []string{"Bearer fixture"}},
		&output,
	)
	if err != nil {
		t.Fatalf("perform typed request: %v", err)
	}
	if !output.Ready {
		t.Fatalf("expected decoded readiness response")
	}
}

func TestJSONHTTPClientReturnsTypedStatusWithoutLeakingBody(t *testing.T) {
	const secret = "response-secret-sentinel"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, secret, http.StatusUnauthorized)
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test URL: %v", err)
	}
	client := NewJSONHTTPClient(server.Client(), baseURL, "fixture")
	err = client.Do(
		context.Background(),
		http.MethodGet,
		"/api/items",
		nil,
		nil,
		nil,
		nil,
	)
	var statusError HTTPStatusError
	if !errors.As(err, &statusError) || statusError.Code != http.StatusUnauthorized {
		t.Fatalf("expected typed HTTP 401, got %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("status error leaked response body")
	}
}

func TestJSONHTTPClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(strings.Repeat("x", maxInfraResponseBytes+1)))
	}))
	defer server.Close()

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test URL: %v", err)
	}
	client := NewJSONHTTPClient(server.Client(), baseURL, "fixture")
	err = client.Do(
		context.Background(),
		http.MethodGet,
		"/api/items",
		nil,
		nil,
		nil,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("expected bounded response failure, got %v", err)
	}
}
