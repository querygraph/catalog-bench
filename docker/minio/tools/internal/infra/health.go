package infra

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// HealthcheckURL returns the configured endpoint or the in-container default.
func HealthcheckURL(getenv func(string) string) string {
	return valueOr(getenv, "MINIO_HEALTHCHECK_URL", "http://127.0.0.1:9000/minio/health/ready")
}

// CheckReady performs one bounded readiness request.
func CheckReady(ctx context.Context, client *http.Client, endpoint string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("construct readiness request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request MinIO readiness: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("MinIO readiness returned %s", strings.TrimSpace(response.Status))
	}
	return nil
}
