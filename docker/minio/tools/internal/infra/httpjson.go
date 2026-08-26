package infra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxInfraResponseBytes = 1 << 20

// HTTPStatusError preserves machine-checkable status without copying response
// bodies—which may contain deployment details or credentials—into logs.
type HTTPStatusError struct {
	Method string
	Path   string
	Code   int
}

func (err HTTPStatusError) Error() string {
	return fmt.Sprintf("%s %s returned HTTP %d", err.Method, err.Path, err.Code)
}

// JSONHTTPClient is the shared bounded transport for typed infrastructure
// reconcilers. Callers own API-specific request and response models.
type JSONHTTPClient struct {
	httpClient *http.Client
	baseURL    *url.URL
	service    string
}

func NewJSONHTTPClient(httpClient *http.Client, baseURL *url.URL, service string) JSONHTTPClient {
	return JSONHTTPClient{httpClient: httpClient, baseURL: baseURL, service: service}
}

func (client JSONHTTPClient) Do(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	payload []byte,
	headers http.Header,
	output any,
) error {
	endpoint := *client.baseURL
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + path
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("construct %s request: %w", client.service, err)
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	if payload != nil && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Accept", "application/json")

	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("request %s: %w", client.service, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return HTTPStatusError{Method: method, Path: path, Code: response.StatusCode}
	}

	limited := io.LimitReader(response.Body, maxInfraResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read %s response: %w", client.service, err)
	}
	if len(body) > maxInfraResponseBytes {
		return fmt.Errorf("%s response exceeded %d bytes", client.service, maxInfraResponseBytes)
	}
	if output == nil || len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, output); err != nil {
		return fmt.Errorf("decode %s response: %w", client.service, err)
	}
	return nil
}
