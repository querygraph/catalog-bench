package infra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

const defaultPolarisS3RoleARN = "arn:aws:iam::000000000000:role/polaris-bench"

// PolarisSettings is the validated fixture boundary for OAuth, management, and
// client-facing catalog readiness.
type PolarisSettings struct {
	BaseURL             *url.URL
	Realm               string
	ClientID            string
	ClientSecret        string
	Scope               string
	Catalog             string
	DefaultBaseLocation string
	S3Endpoint          string
	S3STSEndpoint       string
	S3RoleARN           string
	S3Region            string
}

func LoadPolarisSettings(getenv func(string) string) (PolarisSettings, error) {
	baseURL, err := parsePolarisBaseURL(valueOr(getenv, "POLARIS_URL", "http://polaris:8181"))
	if err != nil {
		return PolarisSettings{}, err
	}
	s3Endpoint := valueOr(getenv, "POLARIS_S3_ENDPOINT", "http://minio:9000")
	settings := PolarisSettings{
		BaseURL:             baseURL,
		Realm:               valueOr(getenv, "POLARIS_REALM", "POLARIS"),
		ClientID:            strings.TrimSpace(getenv("POLARIS_CLIENT_ID")),
		ClientSecret:        strings.TrimSpace(getenv("POLARIS_CLIENT_SECRET")),
		Scope:               valueOr(getenv, "POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
		Catalog:             valueOr(getenv, "POLARIS_CATALOG", "bench"),
		DefaultBaseLocation: valueOr(getenv, "POLARIS_BASE_LOCATION", "s3://warehouse/bench"),
		S3Endpoint:          s3Endpoint,
		S3STSEndpoint:       valueOr(getenv, "POLARIS_S3_STS_ENDPOINT", s3Endpoint),
		S3RoleARN: valueOr(
			getenv,
			"POLARIS_S3_ROLE_ARN",
			defaultPolarisS3RoleARN,
		),
		S3Region: valueOr(getenv, "POLARIS_S3_REGION", "us-east-1"),
	}

	required := []struct {
		name  string
		value string
	}{
		{"POLARIS_REALM", settings.Realm},
		{"POLARIS_CLIENT_ID", settings.ClientID},
		{"POLARIS_CLIENT_SECRET", settings.ClientSecret},
		{"POLARIS_SCOPE", settings.Scope},
		{"POLARIS_CATALOG", settings.Catalog},
		{"POLARIS_BASE_LOCATION", settings.DefaultBaseLocation},
		{"POLARIS_S3_ENDPOINT", settings.S3Endpoint},
		{"POLARIS_S3_STS_ENDPOINT", settings.S3STSEndpoint},
		{"POLARIS_S3_ROLE_ARN", settings.S3RoleARN},
		{"POLARIS_S3_REGION", settings.S3Region},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return PolarisSettings{}, fmt.Errorf("%s must not be empty", field.name)
		}
	}
	if !portableFixtureIdentifier(settings.Realm) || !portableFixtureIdentifier(settings.Catalog) {
		return PolarisSettings{}, fmt.Errorf("POLARIS_REALM and POLARIS_CATALOG must be portable fixture identifiers")
	}
	if err := validateS3Location(settings.DefaultBaseLocation); err != nil {
		return PolarisSettings{}, fmt.Errorf("POLARIS_BASE_LOCATION: %w", err)
	}
	if _, err := parsePolarisBaseURL(settings.S3Endpoint); err != nil {
		return PolarisSettings{}, fmt.Errorf("POLARIS_S3_ENDPOINT: %w", err)
	}
	if _, err := parsePolarisBaseURL(settings.S3STSEndpoint); err != nil {
		return PolarisSettings{}, fmt.Errorf("POLARIS_S3_STS_ENDPOINT: %w", err)
	}
	return settings, nil
}

func parsePolarisBaseURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parse absolute HTTP(S) URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("URL must be absolute and use http or https")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("URL must not contain credentials, a query, or a fragment")
	}
	if strings.Trim(parsed.Path, "/") != "" {
		return nil, fmt.Errorf("URL must not contain a path")
	}
	parsed.Path = ""
	return parsed, nil
}

func portableFixtureIdentifier(value string) bool {
	return value != "" && strings.IndexFunc(value, func(character rune) bool {
		return !(character >= 'a' && character <= 'z') &&
			!(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') &&
			character != '-' && character != '_' && character != '.'
	}) == -1
}

func validateS3Location(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse S3 location: %w", err)
	}
	if parsed.Scheme != "s3" || parsed.Host == "" || strings.Trim(parsed.Path, "/") == "" {
		return fmt.Errorf("must be an s3://bucket/prefix location")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must not contain credentials, a query, or a fragment")
	}
	return nil
}

type PolarisClient struct {
	transport JSONHTTPClient
	settings  PolarisSettings
}

func NewPolarisClient(httpClient *http.Client, settings PolarisSettings) PolarisClient {
	return PolarisClient{
		transport: NewJSONHTTPClient(httpClient, settings.BaseURL, "Polaris"),
		settings:  settings,
	}
}

type polarisTokenResponse struct {
	AccessToken string `json:"access_token"`
}

type polarisCatalog struct {
	Type              string               `json:"type"`
	Name              string               `json:"name"`
	Properties        map[string]string    `json:"properties"`
	StorageConfigInfo polarisStorageConfig `json:"storageConfigInfo"`
}

type polarisStorageConfig struct {
	StorageType      string   `json:"storageType"`
	AllowedLocations []string `json:"allowedLocations"`
	StorageName      *string  `json:"storageName"`
	RoleARN          *string  `json:"roleArn"`
	ExternalID       *string  `json:"externalId"`
	UserARN          *string  `json:"userArn"`
	Region           string   `json:"region"`
	Endpoint         string   `json:"endpoint"`
	EndpointInternal string   `json:"endpointInternal"`
	STSEndpoint      *string  `json:"stsEndpoint"`
	STSUnavailable   bool     `json:"stsUnavailable"`
	PathStyleAccess  bool     `json:"pathStyleAccess"`
	KMSUnavailable   bool     `json:"kmsUnavailable"`
}

type createPolarisCatalogRequest struct {
	Catalog polarisCatalog `json:"catalog"`
}

const (
	polarisCatalogAdminRole       = "catalog_admin"
	polarisCatalogGrantType       = "catalog"
	polarisManageContentPrivilege = "CATALOG_MANAGE_CONTENT"
)

type polarisGrant struct {
	Type      string `json:"type"`
	Privilege string `json:"privilege"`
}

type listPolarisGrantsResponse struct {
	Grants []polarisGrant `json:"grants"`
}

type putPolarisGrantRequest struct {
	Grant polarisGrant `json:"grant"`
}

func (client PolarisClient) desiredCatalog() polarisCatalog {
	roleARN := client.settings.S3RoleARN
	stsEndpoint := client.settings.S3STSEndpoint
	return polarisCatalog{
		Type: "INTERNAL",
		Name: client.settings.Catalog,
		Properties: map[string]string{
			"default-base-location": client.settings.DefaultBaseLocation,
		},
		StorageConfigInfo: polarisStorageConfig{
			StorageType:      "S3",
			AllowedLocations: []string{client.settings.DefaultBaseLocation},
			RoleARN:          &roleARN,
			Region:           client.settings.S3Region,
			Endpoint:         client.settings.S3Endpoint,
			EndpointInternal: client.settings.S3Endpoint,
			STSEndpoint:      &stsEndpoint,
			PathStyleAccess:  true,
			KMSUnavailable:   true,
		},
	}
}

// EnsureCatalog creates the fixture catalog only when absent, reads it back,
// rejects material storage or routing drift, and idempotently reconciles the
// catalog role needed for stock clients to read and write table data.
func (client PolarisClient) EnsureCatalog(ctx context.Context) error {
	token, err := client.acquireToken(ctx)
	if err != nil {
		return err
	}
	desired := client.desiredCatalog()
	actual, found, err := client.getCatalog(ctx, token)
	if err != nil {
		return err
	}
	if !found {
		payload, err := json.Marshal(createPolarisCatalogRequest{Catalog: desired})
		if err != nil {
			return fmt.Errorf("encode Polaris catalog request: %w", err)
		}
		err = client.transport.Do(
			ctx,
			http.MethodPost,
			"/api/management/v1/catalogs",
			nil,
			payload,
			client.authorizedHeaders(token),
			nil,
		)
		if err != nil {
			var statusError HTTPStatusError
			if !errors.As(err, &statusError) || statusError.Code != http.StatusConflict {
				return fmt.Errorf("create Polaris catalog %q: %w", desired.Name, err)
			}
		}

		actual, found, err = client.getCatalog(ctx, token)
		if err != nil {
			return fmt.Errorf("verify Polaris catalog %q: %w", desired.Name, err)
		}
		if !found {
			return fmt.Errorf("Polaris catalog %q is absent after creation", desired.Name)
		}
	}
	if err := comparePolarisCatalog(desired, actual); err != nil {
		return err
	}
	return client.ensureCatalogAdminGrant(ctx, token)
}

func (client PolarisClient) ensureCatalogAdminGrant(ctx context.Context, token string) error {
	desired := desiredPolarisCatalogAdminGrant()
	grants, err := client.listCatalogAdminGrants(ctx, token)
	if err != nil {
		return err
	}
	if slices.Contains(grants, desired) {
		return nil
	}

	payload, err := json.Marshal(putPolarisGrantRequest{Grant: desired})
	if err != nil {
		return fmt.Errorf("encode Polaris catalog-role grant request: %w", err)
	}
	err = client.transport.Do(
		ctx,
		http.MethodPut,
		client.catalogAdminGrantsPath(),
		nil,
		payload,
		client.authorizedHeaders(token),
		nil,
	)
	if err != nil {
		var statusError HTTPStatusError
		if !errors.As(err, &statusError) || statusError.Code != http.StatusConflict {
			return fmt.Errorf(
				"grant Polaris privilege %s to catalog role %q: %w",
				desired.Privilege,
				polarisCatalogAdminRole,
				err,
			)
		}
	}

	grants, err = client.listCatalogAdminGrants(ctx, token)
	if err != nil {
		return fmt.Errorf("verify Polaris catalog-role grant: %w", err)
	}
	if !slices.Contains(grants, desired) {
		return fmt.Errorf(
			"Polaris catalog role %q lacks privilege %s after grant",
			polarisCatalogAdminRole,
			desired.Privilege,
		)
	}
	return nil
}

func (client PolarisClient) listCatalogAdminGrants(
	ctx context.Context,
	token string,
) ([]polarisGrant, error) {
	var response listPolarisGrantsResponse
	if err := client.transport.Do(
		ctx,
		http.MethodGet,
		client.catalogAdminGrantsPath(),
		nil,
		nil,
		client.authorizedHeaders(token),
		&response,
	); err != nil {
		return nil, fmt.Errorf(
			"read grants for Polaris catalog role %q: %w",
			polarisCatalogAdminRole,
			err,
		)
	}
	return response.Grants, nil
}

func (client PolarisClient) catalogAdminGrantsPath() string {
	return polarisCatalogRoleGrantsPath(client.settings.Catalog, polarisCatalogAdminRole)
}

func desiredPolarisCatalogAdminGrant() polarisGrant {
	return polarisGrant{
		Type:      polarisCatalogGrantType,
		Privilege: polarisManageContentPrivilege,
	}
}

func polarisCatalogRoleGrantsPath(catalog, role string) string {
	return "/api/management/v1/catalogs/" + url.PathEscape(catalog) +
		"/catalog-roles/" + url.PathEscape(role) + "/grants"
}

// CheckCatalogReady proves the authenticated client-facing config route for the
// same catalog selected by the profile adapter.
func (client PolarisClient) CheckCatalogReady(ctx context.Context) error {
	token, err := client.acquireToken(ctx)
	if err != nil {
		return err
	}
	var config map[string]json.RawMessage
	if err := client.transport.Do(
		ctx,
		http.MethodGet,
		"/api/catalog/v1/config",
		url.Values{"warehouse": []string{client.settings.Catalog}},
		nil,
		client.authorizedHeaders(token),
		&config,
	); err != nil {
		return fmt.Errorf("read Polaris catalog config: %w", err)
	}
	if config == nil {
		return fmt.Errorf("Polaris catalog config response must be a JSON object")
	}
	return nil
}

func (client PolarisClient) acquireToken(ctx context.Context) (string, error) {
	payload := []byte(url.Values{
		"grant_type":    []string{"client_credentials"},
		"client_id":     []string{client.settings.ClientID},
		"client_secret": []string{client.settings.ClientSecret},
		"scope":         []string{client.settings.Scope},
	}.Encode())
	headers := http.Header{
		"Content-Type":  []string{"application/x-www-form-urlencoded"},
		"Polaris-Realm": []string{client.settings.Realm},
	}
	var response polarisTokenResponse
	if err := client.transport.Do(
		ctx,
		http.MethodPost,
		"/api/catalog/v1/oauth/tokens",
		nil,
		payload,
		headers,
		&response,
	); err != nil {
		return "", fmt.Errorf("acquire Polaris token: %w", err)
	}
	token := strings.TrimSpace(response.AccessToken)
	if token == "" {
		return "", fmt.Errorf("Polaris token response omitted access_token")
	}
	return token, nil
}

func (client PolarisClient) getCatalog(
	ctx context.Context,
	token string,
) (polarisCatalog, bool, error) {
	path := "/api/management/v1/catalogs/" + url.PathEscape(client.settings.Catalog)
	var catalog polarisCatalog
	err := client.transport.Do(
		ctx,
		http.MethodGet,
		path,
		nil,
		nil,
		client.authorizedHeaders(token),
		&catalog,
	)
	if err == nil {
		return catalog, true, nil
	}
	var statusError HTTPStatusError
	if errors.As(err, &statusError) && statusError.Code == http.StatusNotFound {
		return polarisCatalog{}, false, nil
	}
	return polarisCatalog{}, false, fmt.Errorf("read Polaris catalog %q: %w", client.settings.Catalog, err)
}

func (client PolarisClient) authorizedHeaders(token string) http.Header {
	return http.Header{
		"Authorization": []string{"Bearer " + token},
		"Polaris-Realm": []string{client.settings.Realm},
	}
}

func comparePolarisCatalog(desired, actual polarisCatalog) error {
	desiredLocations := slices.Clone(desired.StorageConfigInfo.AllowedLocations)
	actualLocations := slices.Clone(actual.StorageConfigInfo.AllowedLocations)
	slices.Sort(desiredLocations)
	slices.Sort(actualLocations)
	matches := desired.Type == actual.Type &&
		desired.Name == actual.Name &&
		maps.Equal(desired.Properties, actual.Properties) &&
		desired.StorageConfigInfo.StorageType == actual.StorageConfigInfo.StorageType &&
		slices.Equal(desiredLocations, actualLocations) &&
		equalOptionalString(desired.StorageConfigInfo.StorageName, actual.StorageConfigInfo.StorageName) &&
		equalOptionalString(desired.StorageConfigInfo.RoleARN, actual.StorageConfigInfo.RoleARN) &&
		equalOptionalString(desired.StorageConfigInfo.ExternalID, actual.StorageConfigInfo.ExternalID) &&
		equalOptionalString(desired.StorageConfigInfo.UserARN, actual.StorageConfigInfo.UserARN) &&
		desired.StorageConfigInfo.Region == actual.StorageConfigInfo.Region &&
		equivalentEndpoint(desired.StorageConfigInfo.Endpoint, actual.StorageConfigInfo.Endpoint) &&
		equivalentEndpoint(
			effectiveInternalEndpoint(desired.StorageConfigInfo),
			effectiveInternalEndpoint(actual.StorageConfigInfo),
		) &&
		equivalentOptionalEndpoint(
			desired.StorageConfigInfo.STSEndpoint,
			actual.StorageConfigInfo.STSEndpoint,
		) &&
		desired.StorageConfigInfo.STSUnavailable == actual.StorageConfigInfo.STSUnavailable &&
		desired.StorageConfigInfo.PathStyleAccess == actual.StorageConfigInfo.PathStyleAccess &&
		desired.StorageConfigInfo.KMSUnavailable == actual.StorageConfigInfo.KMSUnavailable
	if !matches {
		return fmt.Errorf("Polaris catalog %q exists with configuration drift", desired.Name)
	}
	return nil
}

func equivalentEndpoint(left, right string) bool {
	return strings.TrimRight(left, "/") == strings.TrimRight(right, "/")
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equivalentOptionalEndpoint(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return equivalentEndpoint(*left, *right)
}

func effectiveInternalEndpoint(config polarisStorageConfig) string {
	if config.EndpointInternal != "" {
		return config.EndpointInternal
	}
	return config.Endpoint
}

// IsTransientInfrastructureError identifies startup races without retrying
// authentication, validation, or configuration-drift failures.
func IsTransientInfrastructureError(err error) bool {
	var statusError HTTPStatusError
	if errors.As(err, &statusError) {
		return statusError.Code == http.StatusTooManyRequests || statusError.Code >= 500
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}
