// Package faultproxy implements deterministic, benchmark-owned HTTP fault
// injection. It forwards request bytes unchanged and records bounded,
// sanitized observations; rules never inspect or retain headers or bodies.
package faultproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxControlBody = 64 << 10

type Phase string

const (
	BeforeUpstream Phase = "before-upstream"
	DuringUpstream Phase = "during-upstream"
	AfterUpstream  Phase = "after-upstream"
)

type Action string

const (
	Disconnect       Action = "disconnect"
	PauseRequestBody Action = "pause-request-body"
)

type Rule struct {
	ID           string `json:"id"`
	Method       string `json:"method"`
	PathContains string `json:"path_contains"`
	Occurrence   uint64 `json:"occurrence"`
	Injections   uint64 `json:"injections"`
	Phase        Phase  `json:"phase"`
	Action       Action `json:"action"`
}

func (rule Rule) Validate() error {
	if rule.ID == "" || len(rule.ID) > 64 {
		return errors.New("id must contain 1-64 characters")
	}
	for _, character := range rule.ID {
		if !((character >= 'a' && character <= 'z') || character == '-' || (character >= '0' && character <= '9')) {
			return errors.New("id must use lowercase ASCII letters, digits, and hyphens")
		}
	}
	if rule.Method == "" || rule.Method != strings.ToUpper(rule.Method) {
		return errors.New("method must be an uppercase HTTP method")
	}
	if rule.PathContains == "" || !strings.HasPrefix(rule.PathContains, "/") {
		return errors.New("path_contains must be a nonempty absolute-path fragment")
	}
	if strings.ContainsAny(rule.PathContains, "?#") {
		return errors.New("path_contains must not include query text or a fragment")
	}
	if rule.Occurrence == 0 {
		return errors.New("occurrence must be greater than zero")
	}
	if rule.Injections == 0 || rule.Injections > 1000 {
		return errors.New("injections must be from 1 through 1000")
	}
	if rule.Occurrence > ^uint64(0)-rule.Injections+1 {
		return errors.New("occurrence and injections overflow the match range")
	}
	if rule.Phase != BeforeUpstream && rule.Phase != DuringUpstream && rule.Phase != AfterUpstream {
		return errors.New("phase must be before-upstream, during-upstream, or after-upstream")
	}
	if rule.Action != Disconnect && rule.Action != PauseRequestBody {
		return errors.New("action must be disconnect or pause-request-body")
	}
	if rule.Action == PauseRequestBody && rule.Phase != DuringUpstream {
		return errors.New("pause-request-body requires phase during-upstream")
	}
	if rule.Action == Disconnect && rule.Phase == DuringUpstream {
		return errors.New("during-upstream requires action pause-request-body")
	}
	return nil
}

type Event struct {
	Sequence       uint64 `json:"sequence"`
	RuleID         string `json:"rule_id"`
	Phase          Phase  `json:"phase"`
	Action         Action `json:"action"`
	Method         string `json:"method"`
	PathSHA256     string `json:"path_sha256"`
	MatchNumber    uint64 `json:"match_number"`
	UpstreamStatus *int   `json:"upstream_status,omitempty"`
}

type Snapshot struct {
	SchemaVersion string  `json:"schema_version"`
	Rule          *Rule   `json:"rule,omitempty"`
	MatchCount    uint64  `json:"match_count"`
	Events        []Event `json:"events"`
}

type Proxy struct {
	upstream *url.URL
	client   *http.Client
	logger   *slog.Logger

	mu         sync.Mutex
	rule       *Rule
	matchCount uint64
	events     []Event
	sequence   uint64
	release    chan struct{}
	released   bool
}

func New(upstream *url.URL, logger *slog.Logger) (*Proxy, error) {
	if upstream == nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" {
		return nil, errors.New("upstream must be an absolute HTTP(S) URL")
	}
	if upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" {
		return nil, errors.New("upstream must not contain credentials, query text, or a fragment")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Proxy{
		upstream: upstream,
		client: &http.Client{
			Transport: http.DefaultTransport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: logger,
	}, nil
}

func (proxy *Proxy) ProxyHandler() http.Handler { return http.HandlerFunc(proxy.serveProxy) }

func (proxy *Proxy) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/rule", proxy.putRule)
	mux.HandleFunc("DELETE /v1/rule", proxy.deleteRule)
	mux.HandleFunc("POST /v1/release", proxy.releaseRule)
	mux.HandleFunc("GET /v1/state", proxy.getState)
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, "{\"status\":\"ready\"}\n")
	})
	return mux
}

func (proxy *Proxy) serveProxy(writer http.ResponseWriter, request *http.Request) {
	rule, matchNumber, inject := proxy.match(request.Method, request.URL.Path)
	if inject && rule.Action == Disconnect && rule.Phase == BeforeUpstream {
		proxy.record(rule, request.Method, request.URL.EscapedPath(), matchNumber, nil)
		disconnect(writer)
		return
	}

	upstreamRequest := request.Clone(request.Context())
	upstreamRequest.RequestURI = ""
	upstreamRequest.URL.Scheme = proxy.upstream.Scheme
	upstreamRequest.URL.Host = proxy.upstream.Host
	upstreamRequest.URL.Path = joinPath(proxy.upstream.Path, request.URL.Path)
	upstreamRequest.URL.RawPath = ""
	removeHopHeaders(upstreamRequest.Header)
	if inject && rule.Action == PauseRequestBody {
		if upstreamRequest.Body == nil {
			proxy.record(rule, request.Method, request.URL.EscapedPath(), matchNumber, nil)
			http.Error(writer, "pause-request-body requires a request body", http.StatusBadRequest)
			return
		}
		upstreamRequest.Body = &gatedBody{
			body:    upstreamRequest.Body,
			release: proxy.releaseChannel(),
			onPause: func() {
				proxy.record(rule, request.Method, request.URL.EscapedPath(), matchNumber, nil)
			},
		}
	}

	response, err := proxy.client.Do(upstreamRequest)
	if err != nil {
		http.Error(writer, "upstream request failed", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()

	if inject && rule.Action == Disconnect && rule.Phase == AfterUpstream {
		status := response.StatusCode
		_, _ = io.Copy(io.Discard, response.Body)
		proxy.record(rule, request.Method, request.URL.EscapedPath(), matchNumber, &status)
		disconnect(writer)
		return
	}

	removeHopHeaders(response.Header)
	for name, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func (proxy *Proxy) match(method string, path string) (Rule, uint64, bool) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.rule == nil || proxy.rule.Method != method || !strings.Contains(path, proxy.rule.PathContains) {
		return Rule{}, 0, false
	}
	proxy.matchCount++
	lastInjection := proxy.rule.Occurrence + proxy.rule.Injections - 1
	return *proxy.rule, proxy.matchCount, proxy.matchCount >= proxy.rule.Occurrence && proxy.matchCount <= lastInjection
}

func (proxy *Proxy) record(rule Rule, method string, escapedPath string, matchNumber uint64, status *int) {
	digest := sha256.Sum256([]byte(escapedPath))
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	proxy.sequence++
	proxy.events = append(proxy.events, Event{
		Sequence:       proxy.sequence,
		RuleID:         rule.ID,
		Phase:          rule.Phase,
		Action:         rule.Action,
		Method:         method,
		PathSHA256:     "sha256:" + hex.EncodeToString(digest[:]),
		MatchNumber:    matchNumber,
		UpstreamStatus: status,
	})
	proxy.logger.Info("injected deterministic fault", "rule_id", rule.ID, "phase", rule.Phase, "match_number", matchNumber)
}

func (proxy *Proxy) putRule(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxControlBody+1))
	decoder.DisallowUnknownFields()
	var rule Rule
	if err := decoder.Decode(&rule); err != nil {
		http.Error(writer, "invalid rule document", http.StatusBadRequest)
		return
	}
	if err := rule.Validate(); err != nil {
		http.Error(writer, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	proxy.mu.Lock()
	proxy.rule = &rule
	proxy.matchCount = 0
	proxy.events = nil
	proxy.sequence = 0
	proxy.released = false
	if rule.Action == PauseRequestBody {
		proxy.release = make(chan struct{})
	} else {
		proxy.release = nil
	}
	proxy.mu.Unlock()
	writeJSON(writer, http.StatusOK, proxy.snapshot())
}

func (proxy *Proxy) deleteRule(writer http.ResponseWriter, _ *http.Request) {
	proxy.mu.Lock()
	proxy.rule = nil
	proxy.matchCount = 0
	proxy.events = nil
	proxy.sequence = 0
	proxy.release = nil
	proxy.released = false
	proxy.mu.Unlock()
	writeJSON(writer, http.StatusOK, proxy.snapshot())
}

func (proxy *Proxy) releaseRule(writer http.ResponseWriter, _ *http.Request) {
	proxy.mu.Lock()
	if proxy.release == nil || proxy.released {
		proxy.mu.Unlock()
		http.Error(writer, "no paused rule is awaiting release", http.StatusConflict)
		return
	}
	close(proxy.release)
	proxy.released = true
	proxy.mu.Unlock()
	writeJSON(writer, http.StatusOK, proxy.snapshot())
}

func (proxy *Proxy) releaseChannel() <-chan struct{} {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return proxy.release
}

func (proxy *Proxy) getState(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, proxy.snapshot())
}

func (proxy *Proxy) snapshot() Snapshot {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	var rule *Rule
	if proxy.rule != nil {
		copy := *proxy.rule
		rule = &copy
	}
	return Snapshot{
		SchemaVersion: "catalog-bench.fault-proxy-state.v1",
		Rule:          rule,
		MatchCount:    proxy.matchCount,
		Events:        append([]Event(nil), proxy.events...),
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func disconnect(writer http.ResponseWriter) {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		http.Error(writer, "connection fault unavailable", http.StatusServiceUnavailable)
		return
	}
	connection, _, err := hijacker.Hijack()
	if err == nil {
		_ = connection.Close()
	}
}

func joinPath(base string, request string) string {
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(request, "/")
}

func removeHopHeaders(headers http.Header) {
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		headers.Del(name)
	}
}

type gatedBody struct {
	body    io.ReadCloser
	release <-chan struct{}
	onPause func()
	once    sync.Once
	first   bool
}

func (body *gatedBody) Read(buffer []byte) (int, error) {
	if !body.first {
		body.first = true
		if len(buffer) > 1 {
			buffer = buffer[:1]
		}
		return body.body.Read(buffer)
	}
	body.once.Do(body.onPause)
	<-body.release
	return body.body.Read(buffer)
}

func (body *gatedBody) Close() error { return body.body.Close() }

type Servers struct {
	Proxy   *http.Server
	Control *http.Server
}

func NewServers(proxy *Proxy, proxyAddress string, controlAddress string) Servers {
	return Servers{
		Proxy:   &http.Server{Addr: proxyAddress, Handler: proxy.ProxyHandler(), ReadHeaderTimeout: 5 * time.Second},
		Control: &http.Server{Addr: controlAddress, Handler: proxy.ControlHandler(), ReadHeaderTimeout: 5 * time.Second},
	}
}

func (servers Servers) Run(ctx context.Context) error {
	errorsChannel := make(chan error, 2)
	start := func(server *http.Server) {
		listener, err := net.Listen("tcp", server.Addr)
		if err != nil {
			errorsChannel <- fmt.Errorf("listen on %s: %w", server.Addr, err)
			return
		}
		errorsChannel <- server.Serve(listener)
	}
	go start(servers.Proxy)
	go start(servers.Control)
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = servers.Proxy.Shutdown(shutdown)
		_ = servers.Control.Shutdown(shutdown)
		return nil
	case err := <-errorsChannel:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func ParsePort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("port must be an integer from 1 through 65535")
	}
	return port, nil
}
