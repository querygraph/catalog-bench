package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/querygraph/catalog-bench/docker/infra-tools/internal/faultproxy"
)

func main() {
	upstreamValue := flag.String("upstream", "", "absolute HTTP(S) upstream URL")
	proxyAddress := flag.String("listen", ":8080", "proxy listen address")
	controlAddress := flag.String("control-listen", ":8081", "control listen address")
	flag.Parse()
	if *upstreamValue == "" {
		fmt.Fprintln(os.Stderr, "--upstream is required")
		os.Exit(2)
	}
	upstream, err := url.Parse(*upstreamValue)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid --upstream")
		os.Exit(2)
	}
	proxy, err := faultproxy.New(upstream, slog.Default())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := faultproxy.NewServers(proxy, *proxyAddress, *controlAddress).Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
