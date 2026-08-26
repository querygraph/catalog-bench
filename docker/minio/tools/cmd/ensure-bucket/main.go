package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/infra"
)

func main() {
	settings, err := infra.LoadBucketSettings(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	client, err := infra.NewBucketAPI(settings)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := infra.EnsureBucket(ctx, client, settings); err != nil {
		log.Fatal(err)
	}
	log.Printf("bucket %q is ready", settings.Bucket)
}
