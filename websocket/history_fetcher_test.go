package websocket

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ws "github.com/gorilla/websocket"
)

func TestHTTPHistoryFetcher_Fetch(t *testing.T) {
	var gotFrom, gotTo string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFrom = r.URL.Query().Get("from")
		gotTo = r.URL.Query().Get("to")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"seq":2,"data":"b"},{"seq":3,"data":"c"}]`))
	}))
	defer server.Close()

	fetcher := NewHTTPHistoryFetcher(server.URL)
	messages, err := fetcher.Fetch(context.Background(), 2, 3)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if gotFrom != "2" || gotTo != "3" {
		t.Fatalf("expected query from=2&to=3, got from=%s&to=%s", gotFrom, gotTo)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	for i, msg := range messages {
		if msg.MsgType != ws.TextMessage {
			t.Fatalf("message %d: expected text message type, got %d", i, msg.MsgType)
		}
	}
	if string(messages[0].Payload) != `{"seq":2,"data":"b"}` {
		t.Fatalf("unexpected message payload: %s", messages[0].Payload)
	}
}

func TestHTTPHistoryFetcher_StatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fetcher := NewHTTPHistoryFetcher(server.URL)
	_, err := fetcher.Fetch(context.Background(), 1, 5)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected status error, got %v", err)
	}
}

func TestHTTPHistoryFetcher_InvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not-json"))
	}))
	defer server.Close()

	fetcher := NewHTTPHistoryFetcher(server.URL)
	_, err := fetcher.Fetch(context.Background(), 1, 5)
	if err == nil || !strings.Contains(err.Error(), "failed to decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestHTTPHistoryFetcher_CustomOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		_, _ = w.Write([]byte("ignored"))
	}))
	defer server.Close()

	fetcher := NewHTTPHistoryFetcher(server.URL,
		WithRequestBuilder(func(ctx context.Context, from, to uint64) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
		}),
		WithResponseDecoder(func(body io.Reader) ([]Message, error) {
			return []Message{testMessage(1, "a")}, nil
		}),
	)
	messages, err := fetcher.Fetch(context.Background(), 1, 1)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
}

func TestHTTPHistoryFetcher_Cancellation(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	fetcher := NewHTTPHistoryFetcher(server.URL)
	fetchErr := make(chan error, 1)
	go func() {
		_, err := fetcher.Fetch(ctx, 1, 5)
		fetchErr <- err
	}()

	<-requestStarted
	cancel()
	if err := <-fetchErr; err == nil {
		t.Fatal("expected the fetch to be canceled")
	}
}
