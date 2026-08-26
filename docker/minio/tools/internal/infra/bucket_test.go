package infra

import (
	"context"
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
)

type bucketStub struct {
	existsResults []bool
	existsErrs    []error
	createErr     error
	existsCalls   int
	createCalls   int
}

func (stub *bucketStub) BucketExists(context.Context, string) (bool, error) {
	index := stub.existsCalls
	stub.existsCalls++
	return stub.existsResults[index], stub.existsErrs[index]
}

func (stub *bucketStub) MakeBucket(context.Context, string, minio.MakeBucketOptions) error {
	stub.createCalls++
	return stub.createErr
}

func TestLoadBucketSettingsNormalizesEndpoint(t *testing.T) {
	values := map[string]string{
		"MINIO_ENDPOINT":   " https://minio.example:9443 ",
		"MINIO_BUCKET":     " warehouse ",
		"MINIO_ACCESS_KEY": " access ",
		"MINIO_SECRET_KEY": " secret ",
	}
	settings, err := LoadBucketSettings(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.Endpoint != "minio.example:9443" || !settings.Secure {
		t.Fatalf("unexpected endpoint settings: %#v", settings)
	}
	if settings.Region != "us-east-1" || settings.Bucket != "warehouse" {
		t.Fatalf("unexpected defaults or normalization: %#v", settings)
	}
}

func TestLoadBucketSettingsRejectsEndpointPaths(t *testing.T) {
	values := map[string]string{
		"MINIO_ENDPOINT":   "http://minio:9000/path",
		"MINIO_BUCKET":     "warehouse",
		"MINIO_ACCESS_KEY": "access",
		"MINIO_SECRET_KEY": "secret",
	}
	if _, err := LoadBucketSettings(func(name string) string { return values[name] }); err == nil {
		t.Fatal("expected endpoint path to be rejected")
	}
}

func TestLoadBucketSettingsRejectsEndpointCredentials(t *testing.T) {
	values := map[string]string{
		"MINIO_ENDPOINT":   "http://user:secret@minio:9000",
		"MINIO_BUCKET":     "warehouse",
		"MINIO_ACCESS_KEY": "access",
		"MINIO_SECRET_KEY": "secret",
	}
	if _, err := LoadBucketSettings(func(name string) string { return values[name] }); err == nil {
		t.Fatal("expected endpoint credentials to be rejected")
	}
}

func TestEnsureBucketSkipsExistingBucket(t *testing.T) {
	stub := &bucketStub{existsResults: []bool{true}, existsErrs: []error{nil}}
	if err := EnsureBucket(context.Background(), stub, BucketSettings{Bucket: "warehouse"}); err != nil {
		t.Fatalf("ensure existing bucket: %v", err)
	}
	if stub.createCalls != 0 {
		t.Fatalf("expected no create call, got %d", stub.createCalls)
	}
}

func TestEnsureBucketAcceptsConcurrentCreationAfterProbe(t *testing.T) {
	stub := &bucketStub{
		existsResults: []bool{false, true},
		existsErrs:    []error{nil, nil},
		createErr:     errors.New("already exists"),
	}
	if err := EnsureBucket(context.Background(), stub, BucketSettings{Bucket: "warehouse"}); err != nil {
		t.Fatalf("ensure concurrently created bucket: %v", err)
	}
	if stub.existsCalls != 2 || stub.createCalls != 1 {
		t.Fatalf("unexpected calls: exists=%d create=%d", stub.existsCalls, stub.createCalls)
	}
}

func TestEnsureBucketPreservesCreateFailure(t *testing.T) {
	stub := &bucketStub{
		existsResults: []bool{false, false},
		existsErrs:    []error{nil, nil},
		createErr:     errors.New("permission denied"),
	}
	if err := EnsureBucket(context.Background(), stub, BucketSettings{Bucket: "warehouse"}); err == nil {
		t.Fatal("expected create failure")
	}
}
