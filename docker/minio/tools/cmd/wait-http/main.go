package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/infra"
)

func main() {
	if err := run(os.Getenv); err != nil {
		log.Fatal(err)
	}
}

func run(getenv func(string) string) error {
	endpoint := strings.TrimSpace(getenv("READY_URL"))
	if endpoint == "" {
		return fmt.Errorf("READY_URL must not be empty")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("READY_URL must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("READY_URL must not contain credentials or a fragment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	return infra.WaitReady(
		ctx,
		&http.Client{Timeout: 5 * time.Second},
		endpoint,
		500*time.Millisecond,
	)
}
