package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/infra"
)

const usage = "usage: polaris-setup ensure | ready"

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, getenv func(string) string) error {
	if len(args) != 1 || (args[0] != "ensure" && args[0] != "ready") {
		return fmt.Errorf("%s", usage)
	}
	settings, err := infra.LoadPolarisSettings(getenv)
	if err != nil {
		return err
	}
	client := infra.NewPolarisClient(&http.Client{Timeout: 10 * time.Second}, settings)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	operation := client.EnsureCatalog
	if args[0] == "ready" {
		operation = client.CheckCatalogReady
	}
	return retryTransient(ctx, operation)
}

func retryTransient(ctx context.Context, operation func(context.Context) error) error {
	for {
		err := operation(ctx)
		if err == nil || !infra.IsTransientInfrastructureError(err) {
			return err
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for Polaris readiness: %w (last operation: %v)", ctx.Err(), err)
		case <-timer.C:
		}
	}
}
