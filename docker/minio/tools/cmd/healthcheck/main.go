package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/infra"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	if err := infra.CheckReady(ctx, client, infra.HealthcheckURL(os.Getenv)); err != nil {
		log.Fatal(err)
	}
}
