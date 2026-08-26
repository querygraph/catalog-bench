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

const usage = "usage: lakekeeper-setup bootstrap PAYLOAD | warehouse PAYLOAD | ready WAREHOUSE"

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, getenv func(string) string) error {
	if len(args) != 2 {
		return fmt.Errorf("%s", usage)
	}

	settings, err := infra.LoadLakekeeperSettings(getenv)
	if err != nil {
		return err
	}
	client := infra.NewLakekeeperClient(
		&http.Client{Timeout: 10 * time.Second},
		settings.BaseURL,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	switch args[0] {
	case "bootstrap":
		payload, err := os.ReadFile(args[1])
		if err != nil {
			return fmt.Errorf("read bootstrap payload: %w", err)
		}
		return client.EnsureBootstrap(ctx, payload, settings.ExpectedVersion)
	case "warehouse":
		payload, err := os.ReadFile(args[1])
		if err != nil {
			return fmt.Errorf("read warehouse payload: %w", err)
		}
		return client.EnsureWarehouse(ctx, payload)
	case "ready":
		return client.CheckCatalogReady(ctx, args[1])
	default:
		return fmt.Errorf("unknown operation %q; %s", args[0], usage)
	}
}
