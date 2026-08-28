package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/faultproxy"
)

type output struct {
	SchemaVersion      string              `json:"schema_version"`
	Phase              faultproxy.Phase    `json:"phase"`
	ClientDisconnected bool                `json:"client_disconnected"`
	ObjectPersisted    bool                `json:"object_persisted"`
	ObjectPathSHA256   string              `json:"object_path_sha256"`
	ContentSHA256      string              `json:"content_sha256"`
	ProxyState         faultproxy.Snapshot `json:"proxy_state"`
}

func main() {
	phaseValue := flag.String("phase", "", "before-upstream or after-upstream")
	proxyEndpoint := flag.String("proxy-endpoint", "", "fault proxy endpoint without a path")
	directEndpoint := flag.String("direct-endpoint", "", "direct object-store endpoint without a path")
	controlURL := flag.String("control-url", "", "fault proxy control base URL")
	bucket := flag.String("bucket", "warehouse", "fixture bucket")
	object := flag.String("object", "fault-probe/metadata/00001.json", "run-owned object key")
	accessKey := flag.String("access-key", "", "benchmark fixture access key")
	secretKey := flag.String("secret-key", "", "benchmark fixture secret key")
	region := flag.String("region", "us-east-1", "object-store region")
	flag.Parse()

	phase := faultproxy.Phase(*phaseValue)
	if phase != faultproxy.BeforeUpstream && phase != faultproxy.AfterUpstream {
		fatal("--phase must be before-upstream or after-upstream")
	}
	if *accessKey == "" || *secretKey == "" {
		fatal("--access-key and --secret-key are required benchmark fixture credentials")
	}
	if err := validateFixtureName(*bucket, *object); err != nil {
		fatal(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := run(ctx, phase, *proxyEndpoint, *directEndpoint, *controlURL, *bucket, *object, *accessKey, *secretKey, *region)
	if err != nil {
		fatal(err.Error())
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fatal("encode evidence")
	}
}

func run(ctx context.Context, phase faultproxy.Phase, proxyEndpoint string, directEndpoint string, controlURL string, bucket string, object string, accessKey string, secretKey string, region string) (output, error) {
	directURL, err := parseEndpoint(directEndpoint)
	if err != nil {
		return output{}, fmt.Errorf("direct endpoint: %w", err)
	}
	proxyURL, err := parseEndpoint(proxyEndpoint)
	if err != nil {
		return output{}, fmt.Errorf("proxy endpoint: %w", err)
	}
	control, err := parseEndpoint(controlURL)
	if err != nil {
		return output{}, fmt.Errorf("control URL: %w", err)
	}

	client, err := minio.New(directURL.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       directURL.Scheme == "https",
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return output{}, fmt.Errorf("construct direct object-store client: %w", err)
	}
	_ = client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{})

	presigned, err := client.PresignedPutObject(ctx, bucket, object, 5*time.Minute)
	if err != nil {
		return output{}, fmt.Errorf("presign metadata PUT: %w", err)
	}
	rule := faultproxy.Rule{
		ID:           "metadata-" + strings.TrimSuffix(string(phase), "-upstream"),
		Method:       http.MethodPut,
		PathContains: "/" + bucket + "/" + object,
		Occurrence:   1,
		Injections:   100,
		Phase:        phase,
		Action:       faultproxy.Disconnect,
	}
	if err := putRule(ctx, control, rule); err != nil {
		return output{}, err
	}

	content := []byte("{\"format-version\":2,\"catalog-bench-fault-probe\":true}\n")
	requestURL := *presigned
	requestURL.Scheme = proxyURL.Scheme
	requestURL.Host = proxyURL.Host
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL.String(), bytes.NewReader(content))
	if err != nil {
		return output{}, fmt.Errorf("construct proxied metadata PUT: %w", err)
	}
	request.Host = presigned.Host
	request.Header.Set("Content-Type", "application/octet-stream")
	httpClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	response, requestErr := httpClient.Do(request)
	if response != nil {
		response.Body.Close()
	}
	clientDisconnected := requestErr != nil
	if !clientDisconnected {
		return output{}, errors.New("faulted metadata PUT unexpectedly returned a response")
	}

	_, statErr := client.StatObject(ctx, bucket, object, minio.StatObjectOptions{})
	persisted := statErr == nil
	if statErr != nil {
		response := minio.ToErrorResponse(statErr)
		if response.StatusCode != http.StatusNotFound && response.Code != "NoSuchKey" {
			return output{}, fmt.Errorf("observe metadata object: %w", statErr)
		}
	}
	state, err := getState(ctx, control)
	if err != nil {
		return output{}, err
	}
	if len(state.Events) != 1 || state.Events[0].RuleID != rule.ID {
		return output{}, fmt.Errorf("proxy recorded %d matching fault events, want exactly one", len(state.Events))
	}
	wantPersisted := phase == faultproxy.AfterUpstream
	if persisted != wantPersisted {
		return output{}, fmt.Errorf("persistence=%t after %s fault, want %t", persisted, phase, wantPersisted)
	}
	_ = client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{})

	pathDigest := sha256.Sum256([]byte("/" + bucket + "/" + object))
	contentDigest := sha256.Sum256(content)
	return output{
		SchemaVersion:      "catalog-bench.object-fault-probe.v1",
		Phase:              phase,
		ClientDisconnected: clientDisconnected,
		ObjectPersisted:    persisted,
		ObjectPathSHA256:   "sha256:" + hex.EncodeToString(pathDigest[:]),
		ContentSHA256:      "sha256:" + hex.EncodeToString(contentDigest[:]),
		ProxyState:         state,
	}, nil
}

func putRule(ctx context.Context, control *url.URL, rule faultproxy.Rule) error {
	body, err := json.Marshal(rule)
	if err != nil {
		return err
	}
	endpoint := *control
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + "/v1/rule"
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("configure fault proxy: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("configure fault proxy returned HTTP %d", response.StatusCode)
	}
	return nil
}

func getState(ctx context.Context, control *url.URL) (faultproxy.Snapshot, error) {
	endpoint := *control
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + "/v1/state"
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return faultproxy.Snapshot{}, fmt.Errorf("read fault proxy state: %w", err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 1<<20)
	var state faultproxy.Snapshot
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return faultproxy.Snapshot{}, fmt.Errorf("decode fault proxy state: %w", err)
	}
	return state, nil
}

func parseEndpoint(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("must not contain credentials, a path, query text, or a fragment")
	}
	return parsed, nil
}

func validateFixtureName(bucket string, object string) error {
	if bucket == "" || object == "" || strings.HasPrefix(object, "/") || strings.ContainsAny(bucket+object, "?#") || !strings.Contains(object, "/metadata/") {
		return errors.New("bucket/object must be nonempty, query-free, relative, and object must contain /metadata/")
	}
	return nil
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, "error:", message)
	os.Exit(1)
}
