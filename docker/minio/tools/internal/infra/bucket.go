package infra

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// BucketSettings is the validated boundary between environment configuration
// and the bucket initializer's operational core.
type BucketSettings struct {
	Endpoint  string
	Secure    bool
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// BucketAPI is the smallest MinIO capability needed for idempotent setup.
type BucketAPI interface {
	BucketExists(context.Context, string) (bool, error)
	MakeBucket(context.Context, string, minio.MakeBucketOptions) error
}

// LoadBucketSettings parses and validates environment-backed settings once.
func LoadBucketSettings(getenv func(string) string) (BucketSettings, error) {
	endpoint, secure, err := parseEndpoint(valueOr(getenv, "MINIO_ENDPOINT", "http://minio:9000"))
	if err != nil {
		return BucketSettings{}, err
	}

	settings := BucketSettings{
		Endpoint:  endpoint,
		Secure:    secure,
		Region:    valueOr(getenv, "MINIO_REGION", "us-east-1"),
		Bucket:    strings.TrimSpace(getenv("MINIO_BUCKET")),
		AccessKey: strings.TrimSpace(getenv("MINIO_ACCESS_KEY")),
		SecretKey: strings.TrimSpace(getenv("MINIO_SECRET_KEY")),
	}

	required := []struct {
		name  string
		value string
	}{
		{"MINIO_BUCKET", settings.Bucket},
		{"MINIO_ACCESS_KEY", settings.AccessKey},
		{"MINIO_SECRET_KEY", settings.SecretKey},
	}
	for _, field := range required {
		if field.value == "" {
			return BucketSettings{}, fmt.Errorf("%s must not be empty", field.name)
		}
	}

	return settings, nil
}

func parseEndpoint(raw string) (string, bool, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false, fmt.Errorf("parse MINIO_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", false, fmt.Errorf("MINIO_ENDPOINT must use http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false, fmt.Errorf("MINIO_ENDPOINT must contain only scheme and authority")
	}
	return parsed.Host, parsed.Scheme == "https", nil
}

func valueOr(getenv func(string) string, name, fallback string) string {
	if value := strings.TrimSpace(getenv(name)); value != "" {
		return value
	}
	return fallback
}

// NewBucketAPI constructs the production adapter after settings validation.
func NewBucketAPI(settings BucketSettings) (*minio.Client, error) {
	client, err := minio.New(settings.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(settings.AccessKey, settings.SecretKey, ""),
		Secure: settings.Secure,
		Region: settings.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("construct MinIO client: %w", err)
	}
	return client, nil
}

// EnsureBucket creates a missing bucket and treats a concurrent creator as
// success only after a second existence check.
func EnsureBucket(ctx context.Context, api BucketAPI, settings BucketSettings) error {
	exists, err := api.BucketExists(ctx, settings.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", settings.Bucket, err)
	}
	if exists {
		return nil
	}

	createErr := api.MakeBucket(ctx, settings.Bucket, minio.MakeBucketOptions{
		Region: settings.Region,
	})
	if createErr == nil {
		return nil
	}

	exists, probeErr := api.BucketExists(ctx, settings.Bucket)
	if probeErr == nil && exists {
		return nil
	}
	if probeErr != nil {
		return fmt.Errorf(
			"create bucket %q: %w; verify concurrent creation: %v",
			settings.Bucket,
			createErr,
			probeErr,
		)
	}
	return fmt.Errorf("create bucket %q: %w", settings.Bucket, createErr)
}
