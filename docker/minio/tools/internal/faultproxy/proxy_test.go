package faultproxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBeforeAndAfterUpstreamDisconnectHaveDistinctPersistence(t *testing.T) {
	var admitted atomic.Uint64
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		admitted.Add(1)
		writer.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := New(parsed, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy.ProxyHandler())
	defer server.Close()
	control := httptest.NewServer(proxy.ControlHandler())
	defer control.Close()

	for _, test := range []struct {
		phase          Phase
		wantAdmitted   uint64
		wantStatusSeen bool
	}{
		{phase: BeforeUpstream, wantAdmitted: 0, wantStatusSeen: false},
		{phase: AfterUpstream, wantAdmitted: 1, wantStatusSeen: true},
	} {
		t.Run(string(test.phase), func(t *testing.T) {
			admitted.Store(0)
			putRule(t, control.URL, Rule{
				ID:           "metadata-loss",
				Method:       "PUT",
				PathContains: "/metadata/",
				Occurrence:   1,
				Injections:   1,
				Phase:        test.phase,
				Action:       Disconnect,
			})

			request, err := http.NewRequest(http.MethodPut, server.URL+"/warehouse/table/metadata/00001.json?credential=never-record", strings.NewReader("private-metadata"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "secret-never-record")
			response, err := http.DefaultClient.Do(request)
			if err == nil {
				response.Body.Close()
				t.Fatal("expected a disconnected response")
			}
			if got := admitted.Load(); got != test.wantAdmitted {
				t.Fatalf("upstream admissions = %d, want %d", got, test.wantAdmitted)
			}

			state := getState(t, control.URL)
			if len(state.Events) != 1 {
				t.Fatalf("events = %d, want 1", len(state.Events))
			}
			if (state.Events[0].UpstreamStatus != nil) != test.wantStatusSeen {
				t.Fatalf("upstream status presence = %v, want %v", state.Events[0].UpstreamStatus != nil, test.wantStatusSeen)
			}
			encoded, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{"credential", "private-metadata", "secret-never-record", "00001.json"} {
				if bytes.Contains(encoded, []byte(private)) {
					t.Fatalf("state leaked %q", private)
				}
			}
		})
	}
}

func TestOccurrenceAndNormalForwarding(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Upstream", "yes")
		_, _ = io.WriteString(writer, "ok")
	}))
	defer upstream.Close()
	parsed, _ := url.Parse(upstream.URL)
	proxy, _ := New(parsed, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(proxy.ProxyHandler())
	defer server.Close()
	control := httptest.NewServer(proxy.ControlHandler())
	defer control.Close()
	putRule(t, control.URL, Rule{ID: "second-post", Method: "POST", PathContains: "/objects/", Occurrence: 2, Injections: 1, Phase: BeforeUpstream, Action: Disconnect})

	response, err := http.Post(server.URL+"/objects/a", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if string(body) != "ok" || response.Header.Get("X-Upstream") != "yes" {
		t.Fatalf("normal forwarding failed: body=%q headers=%v", body, response.Header)
	}
	if response, err = http.Post(server.URL+"/objects/b", "application/octet-stream", nil); err == nil {
		response.Body.Close()
		t.Fatal("second matching request should disconnect")
	}
	state := getState(t, control.URL)
	if state.MatchCount != 2 || len(state.Events) != 1 || state.Events[0].MatchNumber != 2 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestRuleValidationRejectsUnsafeOrAmbiguousInput(t *testing.T) {
	invalid := []Rule{
		{},
		{ID: "UPPER", Method: "PUT", PathContains: "/x", Occurrence: 1, Phase: BeforeUpstream, Action: Disconnect},
		{ID: "valid", Method: "put", PathContains: "/x", Occurrence: 1, Phase: BeforeUpstream, Action: Disconnect},
		{ID: "valid", Method: "PUT", PathContains: "x", Occurrence: 1, Phase: BeforeUpstream, Action: Disconnect},
		{ID: "valid", Method: "PUT", PathContains: "/x?secret=y", Occurrence: 1, Phase: BeforeUpstream, Action: Disconnect},
		{ID: "valid", Method: "PUT", PathContains: "/x", Occurrence: 0, Phase: BeforeUpstream, Action: Disconnect},
		{ID: "valid", Method: "PUT", PathContains: "/x", Occurrence: 1, Injections: 0, Phase: BeforeUpstream, Action: Disconnect},
	}
	for _, rule := range invalid {
		if err := rule.Validate(); err == nil {
			t.Fatalf("expected invalid rule: %+v", rule)
		}
	}
}

func TestBoundedInjectionRangeSurvivesAutomaticRetries(t *testing.T) {
	var admitted atomic.Uint64
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		admitted.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	parsed, _ := url.Parse(upstream.URL)
	proxy, _ := New(parsed, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(proxy.ProxyHandler())
	defer server.Close()
	control := httptest.NewServer(proxy.ControlHandler())
	defer control.Close()
	putRule(t, control.URL, Rule{ID: "three-posts", Method: "POST", PathContains: "/commit", Occurrence: 1, Injections: 3, Phase: BeforeUpstream, Action: Disconnect})

	for index := 0; index < 4; index++ {
		response, err := http.Post(server.URL+"/commit", "application/json", nil)
		if index < 3 {
			if err == nil {
				response.Body.Close()
				t.Fatalf("request %d should disconnect", index+1)
			}
		} else if err != nil {
			t.Fatalf("request after injection range: %v", err)
		} else {
			response.Body.Close()
		}
	}
	state := getState(t, control.URL)
	if state.MatchCount != 4 || len(state.Events) != 3 || admitted.Load() != 1 {
		t.Fatalf("unexpected bounded-range state=%+v admitted=%d", state, admitted.Load())
	}
}

func putRule(t *testing.T, controlURL string, rule Rule) {
	t.Helper()
	body, _ := json.Marshal(rule)
	request, _ := http.NewRequest(http.MethodPut, controlURL+"/v1/rule", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("put rule status=%d body=%q", response.StatusCode, payload)
	}
}

func getState(t *testing.T, controlURL string) Snapshot {
	t.Helper()
	response, err := http.Get(controlURL + "/v1/state")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state Snapshot
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestDisconnectRequiresRealSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener.Close()
}
