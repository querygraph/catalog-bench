package infra

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const matchingWarehouseResponse = `{
  "warehouses": [{
    "name": "bench",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "status": "active",
    "storage-profile": {
      "type": "s3",
      "bucket": "warehouse",
      "key-prefix": "lakekeeper",
      "endpoint": "http://minio:9000/",
      "sts-endpoint": "http://minio:9000/",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": true
    },
    "storage-credential-type": {
      "type": "s3",
      "credential-type": "access-key"
    }
  }]
}`

const warehouseRequestJSON = `{
  "warehouse-name": "bench",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "warehouse",
    "key-prefix": "lakekeeper",
    "endpoint": "http://minio:9000",
    "sts-endpoint": "http://minio:9000",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "fixture",
    "secret-access-key": "fixture"
  }
}`

func TestEnsureBootstrapSkipsVerifiedExistingState(t *testing.T) {
	posts := 0
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			posts++
		}
		writeJSON(writer, `{"version":"0.13.3","bootstrapped":true}`)
	})
	defer closeServer()

	if err := client.EnsureBootstrap(context.Background(), []byte(`{"accept-terms-of-use":true}`), "0.13.3"); err != nil {
		t.Fatalf("ensure bootstrap: %v", err)
	}
	if posts != 0 {
		t.Fatalf("expected no POST, got %d", posts)
	}
}

func TestEnsureBootstrapCreatesAndRechecksState(t *testing.T) {
	bootstrapped := false
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			writeJSON(writer, fmt.Sprintf(`{"version":"0.13.3","bootstrapped":%t}`, bootstrapped))
		case http.MethodPost:
			bootstrapped = true
			writer.WriteHeader(http.StatusNoContent)
		}
	})
	defer closeServer()

	if err := client.EnsureBootstrap(context.Background(), []byte(`{"accept-terms-of-use":true}`), "0.13.3"); err != nil {
		t.Fatalf("ensure bootstrap: %v", err)
	}
	if !bootstrapped {
		t.Fatal("expected bootstrap POST")
	}
}

func TestEnsureBootstrapRejectsVersionDrift(t *testing.T) {
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, `{"version":"0.14.0","bootstrapped":true}`)
	})
	defer closeServer()

	err := client.EnsureBootstrap(
		context.Background(),
		[]byte(`{"accept-terms-of-use":true}`),
		"0.13.3",
	)
	if err == nil || !strings.Contains(err.Error(), "version drift") {
		t.Fatalf("expected version drift, got %v", err)
	}
}

func TestEnsureBootstrapRejectsMissingTermsAcceptance(t *testing.T) {
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, `{"version":"0.13.3","bootstrapped":false}`)
	})
	defer closeServer()

	err := client.EnsureBootstrap(context.Background(), []byte(`{"accept-terms-of-use":false}`), "0.13.3")
	if err == nil || !strings.Contains(err.Error(), "accept the terms") {
		t.Fatalf("expected terms-acceptance error, got %v", err)
	}
}

func TestEnsureWarehouseAcceptsMatchingStateWithoutPosting(t *testing.T) {
	posts := 0
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			posts++
		}
		writeJSON(writer, matchingWarehouseResponse)
	})
	defer closeServer()

	if err := client.EnsureWarehouse(context.Background(), []byte(warehouseRequestJSON)); err != nil {
		t.Fatalf("ensure warehouse: %v", err)
	}
	if posts != 0 {
		t.Fatalf("expected no POST, got %d", posts)
	}
}

func TestEnsureWarehouseRejectsConfigurationDrift(t *testing.T) {
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, strings.Replace(matchingWarehouseResponse, `"bucket": "warehouse"`, `"bucket": "wrong"`, 1))
	})
	defer closeServer()

	err := client.EnsureWarehouse(context.Background(), []byte(warehouseRequestJSON))
	if err == nil || !strings.Contains(err.Error(), "configuration drift") || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected bucket drift, got %v", err)
	}
}

func TestEnsureWarehouseCreatesAndRechecksState(t *testing.T) {
	created := false
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			created = true
			writer.WriteHeader(http.StatusCreated)
			return
		}
		if created {
			writeJSON(writer, matchingWarehouseResponse)
		} else {
			writeJSON(writer, `{"warehouses":[]}`)
		}
	})
	defer closeServer()

	if err := client.EnsureWarehouse(context.Background(), []byte(warehouseRequestJSON)); err != nil {
		t.Fatalf("ensure warehouse: %v", err)
	}
	if !created {
		t.Fatal("expected warehouse POST")
	}
}

func TestCheckCatalogReadyRequiresPrefixAndConfigEndpoint(t *testing.T) {
	client, closeServer := testLakekeeperClient(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("warehouse") != "bench" {
			http.Error(writer, "unexpected warehouse query", http.StatusBadRequest)
			return
		}
		writeJSON(writer, `{"defaults":{"prefix":"warehouse-id"},"endpoints":["GET /v1/config"]}`)
	})
	defer closeServer()

	if err := client.CheckCatalogReady(context.Background(), "bench"); err != nil {
		t.Fatalf("check catalog ready: %v", err)
	}
}

func TestLoadLakekeeperSettingsRejectsCredentials(t *testing.T) {
	_, err := LoadLakekeeperSettings(func(name string) string {
		if name == "LAKEKEEPER_URL" {
			return "http://user:secret@lakekeeper:8181"
		}
		return ""
	})
	if err == nil {
		t.Fatal("expected embedded credentials to be rejected")
	}
}

func testLakekeeperClient(
	t *testing.T,
	handler http.HandlerFunc,
) (LakekeeperClient, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("parse test URL: %v", err)
	}
	return NewLakekeeperClient(server.Client(), baseURL), server.Close
}

func writeJSON(writer http.ResponseWriter, body string) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(body))
}
