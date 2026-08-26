package infra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
		return fmt.Errorf("request readiness: %w", err)
	}
	defer response.Body.Close()
	observed, readErr := io.CopyN(io.Discard, response.Body, maxInfraResponseBytes+1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("read readiness response: %w", readErr)
	}
	if observed > maxInfraResponseBytes {
		return fmt.Errorf("readiness response exceeded %d bytes", maxInfraResponseBytes)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("readiness returned %s", strings.TrimSpace(response.Status))
	}
	return nil
}

// WaitReady turns a service-start race into an explicit bounded readiness gate.
// It preserves the final status-bearing error when the deadline expires.
func WaitReady(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	interval time.Duration,
) error {
	var lastErr error
	for {
		if err := CheckReady(ctx, client, endpoint); err == nil {
			return nil
		} else {
			lastErr = err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for %s: %w (last probe: %v)", endpoint, ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}
