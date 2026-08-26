package infra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// LakekeeperSettings is the validated environment boundary for management and
// catalog readiness operations.
type LakekeeperSettings struct {
	BaseURL         *url.URL
	ExpectedVersion string
}

// LoadLakekeeperSettings validates the endpoint once before any requests run.
func LoadLakekeeperSettings(getenv func(string) string) (LakekeeperSettings, error) {
	rawURL := valueOr(getenv, "LAKEKEEPER_URL", "http://lakekeeper:8181")
	baseURL, err := url.Parse(rawURL)
	if err != nil {
		return LakekeeperSettings{}, fmt.Errorf("parse LAKEKEEPER_URL: %w", err)
	}
	if (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return LakekeeperSettings{}, fmt.Errorf("LAKEKEEPER_URL must be an absolute HTTP(S) URL")
	}
	if baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return LakekeeperSettings{}, fmt.Errorf("LAKEKEEPER_URL must not contain credentials, a query, or a fragment")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/")

	return LakekeeperSettings{
		BaseURL:         baseURL,
		ExpectedVersion: valueOr(getenv, "LAKEKEEPER_EXPECTED_VERSION", "0.13.3"),
	}, nil
}

// LakekeeperClient reconciles typed management state and verifies the public
// Iceberg REST boundary. It intentionally does not expose a behavior shim.
type LakekeeperClient struct {
	transport JSONHTTPClient
}

// NewLakekeeperClient constructs a client from validated settings.
func NewLakekeeperClient(httpClient *http.Client, baseURL *url.URL) LakekeeperClient {
	return LakekeeperClient{
		transport: NewJSONHTTPClient(httpClient, baseURL, "Lakekeeper"),
	}
}

type lakekeeperInfo struct {
	Version      string `json:"version"`
	Bootstrapped bool   `json:"bootstrapped"`
}

type bootstrapRequest struct {
	AcceptTermsOfUse bool `json:"accept-terms-of-use"`
}

// EnsureBootstrap accepts the terms only when the server reports that it has
// not yet been initialized. Existing state is verified instead of interpreting
// Lakekeeper's HTTP 400 repeat response as success.
func (client LakekeeperClient) EnsureBootstrap(
	ctx context.Context,
	payload []byte,
	expectedVersion string,
) error {
	var request bootstrapRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode bootstrap payload: %w", err)
	}
	if !request.AcceptTermsOfUse {
		return fmt.Errorf("bootstrap payload must explicitly accept the terms of use")
	}

	info, err := client.info(ctx)
	if err != nil {
		return err
	}
	if err := verifyLakekeeperVersion(info.Version, expectedVersion); err != nil {
		return err
	}
	if info.Bootstrapped {
		return nil
	}
	if err := client.post(ctx, "/management/v1/bootstrap", payload); err != nil {
		return fmt.Errorf("bootstrap Lakekeeper: %w", err)
	}

	info, err = client.info(ctx)
	if err != nil {
		return fmt.Errorf("verify Lakekeeper bootstrap: %w", err)
	}
	if !info.Bootstrapped {
		return fmt.Errorf("Lakekeeper remained unbootstrapped after a successful request")
	}
	return verifyLakekeeperVersion(info.Version, expectedVersion)
}

func (client LakekeeperClient) info(ctx context.Context) (lakekeeperInfo, error) {
	var info lakekeeperInfo
	if err := client.get(ctx, "/management/v1/info", nil, &info); err != nil {
		return lakekeeperInfo{}, fmt.Errorf("read Lakekeeper info: %w", err)
	}
	return info, nil
}

func verifyLakekeeperVersion(actual, expected string) error {
	if actual != expected {
		return fmt.Errorf("Lakekeeper version drift: expected %q, got %q", expected, actual)
	}
	return nil
}

type warehouseRequest struct {
	Name              string           `json:"warehouse-name"`
	ProjectID         string           `json:"project-id"`
	StorageProfile    s3StorageProfile `json:"storage-profile"`
	StorageCredential s3Credential     `json:"storage-credential"`
}

type warehouseList struct {
	Warehouses []warehouseState `json:"warehouses"`
}

type warehouseState struct {
	Name                  string           `json:"name"`
	ProjectID             string           `json:"project-id"`
	Status                string           `json:"status"`
	StorageProfile        s3StorageProfile `json:"storage-profile"`
	StorageCredentialType s3Credential     `json:"storage-credential-type"`
}

type s3StorageProfile struct {
	Type            string `json:"type"`
	Bucket          string `json:"bucket"`
	KeyPrefix       string `json:"key-prefix"`
	Endpoint        string `json:"endpoint"`
	STSEndpoint     string `json:"sts-endpoint"`
	Region          string `json:"region"`
	PathStyleAccess bool   `json:"path-style-access"`
	Flavor          string `json:"flavor"`
	STSEnabled      bool   `json:"sts-enabled"`
}

type s3Credential struct {
	Type           string `json:"type"`
	CredentialType string `json:"credential-type"`
}

// EnsureWarehouse creates a missing warehouse and otherwise proves that the
// existing named warehouse has the requested project, S3 profile, and credential
// type. A name collision with drift fails closed.
func (client LakekeeperClient) EnsureWarehouse(ctx context.Context, payload []byte) error {
	var desired warehouseRequest
	if err := json.Unmarshal(payload, &desired); err != nil {
		return fmt.Errorf("decode warehouse payload: %w", err)
	}
	if err := validateWarehouseRequest(desired); err != nil {
		return err
	}

	state, found, err := client.findWarehouse(ctx, desired.Name)
	if err != nil {
		return err
	}
	if found {
		return compareWarehouse(desired, state)
	}

	if err := client.post(ctx, "/management/v1/warehouse", payload); err != nil {
		var statusError HTTPStatusError
		if !errors.As(err, &statusError) || statusError.Code != http.StatusConflict {
			return fmt.Errorf("create warehouse %q: %w", desired.Name, err)
		}
	}

	state, found, err = client.findWarehouse(ctx, desired.Name)
	if err != nil {
		return fmt.Errorf("verify warehouse %q: %w", desired.Name, err)
	}
	if !found {
		return fmt.Errorf("warehouse %q is absent after creation", desired.Name)
	}
	return compareWarehouse(desired, state)
}

func validateWarehouseRequest(request warehouseRequest) error {
	required := []struct {
		name  string
		value string
	}{
		{"warehouse-name", request.Name},
		{"project-id", request.ProjectID},
		{"storage-profile.type", request.StorageProfile.Type},
		{"storage-profile.bucket", request.StorageProfile.Bucket},
		{"storage-profile.endpoint", request.StorageProfile.Endpoint},
		{"storage-profile.region", request.StorageProfile.Region},
		{"storage-credential.type", request.StorageCredential.Type},
		{"storage-credential.credential-type", request.StorageCredential.CredentialType},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("warehouse payload field %s must not be empty", field.name)
		}
	}
	return nil
}

func (client LakekeeperClient) findWarehouse(
	ctx context.Context,
	name string,
) (warehouseState, bool, error) {
	var list warehouseList
	if err := client.get(ctx, "/management/v1/warehouse", nil, &list); err != nil {
		return warehouseState{}, false, fmt.Errorf("list Lakekeeper warehouses: %w", err)
	}
	matches := make([]warehouseState, 0, 1)
	for _, warehouse := range list.Warehouses {
		if warehouse.Name == name {
			matches = append(matches, warehouse)
		}
	}
	if len(matches) > 1 {
		return warehouseState{}, false, fmt.Errorf("multiple warehouses are named %q", name)
	}
	if len(matches) == 0 {
		return warehouseState{}, false, nil
	}
	return matches[0], true, nil
}

func compareWarehouse(desired warehouseRequest, actual warehouseState) error {
	differences := make([]string, 0)
	compare := func(field, expected, observed string) {
		if expected != observed {
			differences = append(differences, fmt.Sprintf("%s expected %q, got %q", field, expected, observed))
		}
	}
	compare("project-id", desired.ProjectID, actual.ProjectID)
	compare("status", "active", actual.Status)
	compare("storage-profile.type", desired.StorageProfile.Type, actual.StorageProfile.Type)
	compare("storage-profile.bucket", desired.StorageProfile.Bucket, actual.StorageProfile.Bucket)
	compare("storage-profile.key-prefix", desired.StorageProfile.KeyPrefix, actual.StorageProfile.KeyPrefix)
	compare("storage-profile.endpoint", normalizeRootURL(desired.StorageProfile.Endpoint), normalizeRootURL(actual.StorageProfile.Endpoint))
	compare("storage-profile.sts-endpoint", normalizeRootURL(desired.StorageProfile.STSEndpoint), normalizeRootURL(actual.StorageProfile.STSEndpoint))
	compare("storage-profile.region", desired.StorageProfile.Region, actual.StorageProfile.Region)
	compare("storage-profile.flavor", desired.StorageProfile.Flavor, actual.StorageProfile.Flavor)
	compare("storage-credential.type", desired.StorageCredential.Type, actual.StorageCredentialType.Type)
	compare("storage-credential.credential-type", desired.StorageCredential.CredentialType, actual.StorageCredentialType.CredentialType)
	if desired.StorageProfile.PathStyleAccess != actual.StorageProfile.PathStyleAccess {
		differences = append(differences, "storage-profile.path-style-access differs")
	}
	if desired.StorageProfile.STSEnabled != actual.StorageProfile.STSEnabled {
		differences = append(differences, "storage-profile.sts-enabled differs")
	}

	if len(differences) != 0 {
		slices.Sort(differences)
		return fmt.Errorf("warehouse %q configuration drift: %s", desired.Name, strings.Join(differences, "; "))
	}
	return nil
}

func normalizeRootURL(value string) string {
	return strings.TrimSuffix(value, "/")
}

type catalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Endpoints []string          `json:"endpoints"`
}

// CheckCatalogReady proves that config negotiation resolves the named warehouse
// and advertises the standard config route.
func (client LakekeeperClient) CheckCatalogReady(ctx context.Context, warehouse string) error {
	if strings.TrimSpace(warehouse) == "" {
		return fmt.Errorf("warehouse must not be empty")
	}
	query := url.Values{"warehouse": []string{warehouse}}
	var config catalogConfig
	if err := client.get(ctx, "/catalog/v1/config", query, &config); err != nil {
		return fmt.Errorf("negotiate catalog config for warehouse %q: %w", warehouse, err)
	}
	if strings.TrimSpace(config.Defaults["prefix"]) == "" {
		return fmt.Errorf("catalog config for warehouse %q omitted its prefix", warehouse)
	}
	if !slices.Contains(config.Endpoints, "GET /v1/config") {
		return fmt.Errorf("catalog config for warehouse %q omitted GET /v1/config", warehouse)
	}
	return nil
}

func (client LakekeeperClient) get(
	ctx context.Context,
	path string,
	query url.Values,
	output any,
) error {
	return client.transport.Do(ctx, http.MethodGet, path, query, nil, nil, output)
}

func (client LakekeeperClient) post(ctx context.Context, path string, payload []byte) error {
	return client.transport.Do(ctx, http.MethodPost, path, nil, payload, nil, nil)
}
