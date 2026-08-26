package infra

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestEnsurePolarisCatalogCreatesAndVerifiesMissingState(t *testing.T) {
	var state *polarisCatalog
	createCalls := 0
	client, closeServer := testPolarisClient(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/catalog/v1/oauth/tokens":
			assertPolarisTokenRequest(t, request)
			writeJSON(writer, `{"access_token":"fixture-token"}`)
		case "/api/management/v1/catalogs/bench":
			assertPolarisAuthorization(t, request)
			if state == nil {
				http.Error(writer, "missing", http.StatusNotFound)
				return
			}
			writeJSONValue(t, writer, *state)
		case "/api/management/v1/catalogs":
			assertPolarisAuthorization(t, request)
			createCalls++
			var payload createPolarisCatalogRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode create request: %v", err)
				return
			}
			state = &payload.Catalog
			writer.WriteHeader(http.StatusCreated)
		default:
			http.Error(writer, "unexpected path", http.StatusNotFound)
		}
	})
	defer closeServer()

	if err := client.EnsureCatalog(context.Background()); err != nil {
		t.Fatalf("ensure Polaris catalog: %v", err)
	}
	if createCalls != 1 || state == nil {
		t.Fatalf("expected one verified catalog creation, calls=%d state=%#v", createCalls, state)
	}
}

func TestEnsurePolarisCatalogAcceptsMatchingStateWithoutPosting(t *testing.T) {
	createCalls := 0
	client, closeServer := testPolarisClient(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/catalog/v1/oauth/tokens":
			writeJSON(writer, `{"access_token":"fixture-token"}`)
		case "/api/management/v1/catalogs/bench":
			writeJSONValue(t, writer, desiredPolarisCatalog())
		case "/api/management/v1/catalogs":
			createCalls++
			writer.WriteHeader(http.StatusCreated)
		default:
			http.Error(writer, "unexpected path", http.StatusNotFound)
		}
	})
	defer closeServer()

	if err := client.EnsureCatalog(context.Background()); err != nil {
		t.Fatalf("ensure existing Polaris catalog: %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("expected no create request, got %d", createCalls)
	}
}

func TestEnsurePolarisCatalogRejectsConfigurationDrift(t *testing.T) {
	drifted := desiredPolarisCatalog()
	drifted.StorageConfigInfo.Endpoint = "http://other-minio:9000"
	client, closeServer := testPolarisClient(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/catalog/v1/oauth/tokens":
			writeJSON(writer, `{"access_token":"fixture-token"}`)
		case "/api/management/v1/catalogs/bench":
			writeJSONValue(t, writer, drifted)
		default:
			http.Error(writer, "unexpected path", http.StatusNotFound)
		}
	})
	defer closeServer()

	err := client.EnsureCatalog(context.Background())
	if err == nil || !strings.Contains(err.Error(), "configuration drift") {
		t.Fatalf("expected configuration drift, got %v", err)
	}
}

func TestCheckPolarisCatalogReadyUsesWarehouseAndBearerToken(t *testing.T) {
	configCalls := 0
	client, closeServer := testPolarisClient(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/catalog/v1/oauth/tokens":
			writeJSON(writer, `{"access_token":"fixture-token"}`)
		case "/api/catalog/v1/config":
			assertPolarisAuthorization(t, request)
			if request.URL.Query().Get("warehouse") != "bench" {
				http.Error(writer, "wrong warehouse", http.StatusBadRequest)
				return
			}
			configCalls++
			writeJSON(writer, `{"defaults":{},"overrides":{}}`)
		default:
			http.Error(writer, "unexpected path", http.StatusNotFound)
		}
	})
	defer closeServer()

	if err := client.CheckCatalogReady(context.Background()); err != nil {
		t.Fatalf("check Polaris readiness: %v", err)
	}
	if configCalls != 1 {
		t.Fatalf("expected one config request, got %d", configCalls)
	}
}

func TestLoadPolarisSettingsRejectsCredentialsInURLs(t *testing.T) {
	_, err := LoadPolarisSettings(func(name string) string {
		switch name {
		case "POLARIS_URL":
			return "http://user:secret@polaris:8181"
		case "POLARIS_CLIENT_ID":
			return "root"
		case "POLARIS_CLIENT_SECRET":
			return "secret"
		default:
			return ""
		}
	})
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("expected embedded credentials to fail, got %v", err)
	}
}

func testPolarisClient(
	t *testing.T,
	handler http.HandlerFunc,
) (PolarisClient, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("parse test URL: %v", err)
	}
	settings := PolarisSettings{
		BaseURL:             baseURL,
		Realm:               "POLARIS",
		ClientID:            "root",
		ClientSecret:        "secret",
		Scope:               "PRINCIPAL_ROLE:ALL",
		Catalog:             "bench",
		DefaultBaseLocation: "s3://warehouse/bench",
		S3Endpoint:          "http://minio:9000",
		S3Region:            "us-east-1",
	}
	return NewPolarisClient(server.Client(), settings), server.Close
}

func desiredPolarisCatalog() polarisCatalog {
	settings := PolarisSettings{
		Catalog:             "bench",
		DefaultBaseLocation: "s3://warehouse/bench",
		S3Endpoint:          "http://minio:9000",
		S3Region:            "us-east-1",
	}
	return (PolarisClient{settings: settings}).desiredCatalog()
}

func assertPolarisTokenRequest(t *testing.T, request *http.Request) {
	t.Helper()
	if err := request.ParseForm(); err != nil {
		t.Errorf("parse token form: %v", err)
		return
	}
	if request.Form.Get("grant_type") != "client_credentials" ||
		request.Form.Get("client_id") != "root" ||
		request.Form.Get("client_secret") != "secret" ||
		request.Form.Get("scope") != "PRINCIPAL_ROLE:ALL" {
		t.Errorf("unexpected token form: %v", request.Form)
	}
	if request.Header.Get("Polaris-Realm") != "POLARIS" {
		t.Errorf("unexpected realm header: %q", request.Header.Get("Polaris-Realm"))
	}
}

func assertPolarisAuthorization(t *testing.T, request *http.Request) {
	t.Helper()
	if request.Header.Get("Authorization") != "Bearer fixture-token" {
		t.Errorf("unexpected authorization: %q", request.Header.Get("Authorization"))
	}
	if request.Header.Get("Polaris-Realm") != "POLARIS" {
		t.Errorf("unexpected realm header: %q", request.Header.Get("Polaris-Realm"))
	}
}

func writeJSONValue(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("encode test response: %v", err)
	}
}
