package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/reugn/go-streams/flow"
)

// wsTestServer is a controllable WebSocket server for tests.
type wsTestServer struct {
	server *httptest.Server
	conns  chan *ws.Conn
}

func newWSTestServer(t *testing.T) *wsTestServer {
	t.Helper()
	testServer := &wsTestServer{conns: make(chan *ws.Conn, 8)}
	upgrader := ws.Upgrader{}
	testServer.server = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			testServer.conns <- conn
		}))
	t.Cleanup(testServer.server.Close)
	return testServer
}

func (s *wsTestServer) url() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

// conn waits for the next client connection accepted by the server.
func (s *wsTestServer) conn(t *testing.T) *ws.Conn {
	t.Helper()
	select {
	case conn := <-s.conns:
		return conn
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for client connection")
		return nil
	}
}

// close closes the server and all accepted connections.
func (s *wsTestServer) close() {
	s.server.Close()
}

// testMessage returns a text message carrying a sequenced test record.
func testMessage(seq uint64, data string) Message {
	return Message{
		MsgType: ws.TextMessage,
		Payload: []byte(fmt.Sprintf(`{"seq":%d,"data":%q}`, seq, data)),
	}
}

// sendRecord writes a sequenced test record to the connection.
func sendRecord(t *testing.T, conn *ws.Conn, seq uint64, data string) {
	t.Helper()
	msg := testMessage(seq, data)
	if err := conn.WriteMessage(msg.MsgType, msg.Payload); err != nil {
		t.Fatalf("failed to send record %d: %v", seq, err)
	}
}

// sendRaw writes a raw payload to the connection.
func sendRaw(t *testing.T, conn *ws.Conn, payload string) {
	t.Helper()
	if err := conn.WriteMessage(ws.TextMessage, []byte(payload)); err != nil {
		t.Fatalf("failed to send payload: %v", err)
	}
}

// extractTestSequence extracts the sequence number from a test record,
// skipping messages without a seq field.
func extractTestSequence(msg Message) (uint64, error) {
	var record struct {
		Seq *uint64 `json:"seq"`
	}
	if err := json.Unmarshal(msg.Payload, &record); err != nil {
		return 0, err
	}
	if record.Seq == nil {
		return 0, ErrSkipMessage
	}
	return *record.Seq, nil
}

// expectRecord asserts that the next output element is the expected record.
func expectRecord(t *testing.T, out <-chan any, seq uint64, data string) {
	t.Helper()
	select {
	case element, ok := <-out:
		if !ok {
			t.Fatalf("output channel closed; expected record %d", seq)
		}
		msg, ok := element.(Message)
		if !ok {
			t.Fatalf("unexpected element type %T", element)
		}
		var record struct {
			Seq  uint64 `json:"seq"`
			Data string `json:"data"`
		}
		if err := json.Unmarshal(msg.Payload, &record); err != nil {
			t.Fatalf("failed to unmarshal record: %v", err)
		}
		if record.Seq != seq || record.Data != data {
			t.Fatalf("expected record (%d, %q), got (%d, %q)", seq, data, record.Seq, record.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for record %d", seq)
	}
}

// expectClosed asserts that the output channel is closed.
func expectClosed(t *testing.T, out <-chan any) {
	t.Helper()
	select {
	case element, ok := <-out:
		if ok {
			t.Fatalf("expected closed output channel, got element %v", element)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the output channel to close")
	}
}

// recordingFetcher serves historical records from a map and records the
// requested ranges. An optional hook can delay or fail the fetch.
type recordingFetcher struct {
	mu      sync.Mutex
	records map[uint64]string
	ranges  [][2]uint64
	hook    func(ctx context.Context) error
}

func (f *recordingFetcher) Fetch(ctx context.Context, from, to uint64) ([]Message, error) {
	f.mu.Lock()
	f.ranges = append(f.ranges, [2]uint64{from, to})
	f.mu.Unlock()
	if f.hook != nil {
		if err := f.hook(ctx); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var messages []Message
	for seq := from; ; seq++ {
		if data, ok := f.records[seq]; ok {
			messages = append(messages, testMessage(seq, data))
		}
		if seq == to {
			break
		}
	}
	return messages, nil
}

func (f *recordingFetcher) requestedRanges() [][2]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]uint64(nil), f.ranges...)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(fetcher HistoryFetcher) *SequencedSourceConfig {
	return &SequencedSourceConfig{
		ExtractSequence: extractTestSequence,
		HistoryFetcher:  fetcher,
		Logger:          testLogger(),
	}
}

func newTestSource(t *testing.T, ctx context.Context, url string,
	config *SequencedSourceConfig) *SequencedSource {
	t.Helper()
	source, err := NewSequencedSource(ctx, url, config)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	return source
}

func TestSequencedSource_InOrderDelivery(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newTestSource(t, ctx, server.url(), testConfig(&recordingFetcher{}))
	conn := server.conn(t)

	for seq := uint64(1); seq <= 5; seq++ {
		sendRecord(t, conn, seq, "tick")
	}
	for seq := uint64(1); seq <= 5; seq++ {
		expectRecord(t, source.Out(), seq, "tick")
	}

	cancel()
	expectClosed(t, source.Out())
	if err := source.Err(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSequencedSource_DuplicatesIgnored(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newTestSource(t, ctx, server.url(), testConfig(&recordingFetcher{}))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 2, "b")
	sendRecord(t, conn, 2, "b") // retransmission of the last delivered record
	sendRecord(t, conn, 1, "a") // retransmission of an older record
	sendRecord(t, conn, 3, "c")

	expectRecord(t, source.Out(), 1, "a")
	expectRecord(t, source.Out(), 2, "b")
	expectRecord(t, source.Out(), 3, "c")
}

func TestSequencedSource_SkipUnsequencedMessages(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newTestSource(t, ctx, server.url(), testConfig(&recordingFetcher{}))
	conn := server.conn(t)

	sendRaw(t, conn, `{"type":"ping"}`)
	sendRecord(t, conn, 1, "a")
	sendRaw(t, conn, `{"type":"pong"}`)
	sendRecord(t, conn, 2, "b")

	expectRecord(t, source.Out(), 1, "a")
	expectRecord(t, source.Out(), 2, "b")
}

func TestSequencedSource_GapBackfill(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &recordingFetcher{records: map[uint64]string{3: "c", 4: "d"}}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 2, "b")
	sendRecord(t, conn, 5, "e")

	for i, data := range []string{"a", "b", "c", "d", "e"} {
		expectRecord(t, source.Out(), uint64(i+1), data)
	}
	ranges := fetcher.requestedRanges()
	if len(ranges) != 1 || ranges[0] != [2]uint64{3, 4} {
		t.Fatalf("expected backfill range [3, 4], got %v", ranges)
	}

	// delivery continues after the backfill
	sendRecord(t, conn, 6, "f")
	expectRecord(t, source.Out(), 6, "f")
}

func TestSequencedSource_LiveRecordsBufferedDuringBackfill(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetcher := &recordingFetcher{
		records: map[uint64]string{3: "c", 4: "d", 5: "e"},
		hook: func(ctx context.Context) error {
			close(fetchStarted)
			select {
			case <-releaseFetch:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 2, "b")
	sendRecord(t, conn, 6, "f")
	expectRecord(t, source.Out(), 1, "a")
	expectRecord(t, source.Out(), 2, "b")
	<-fetchStarted // wait for the backfill request to be in flight

	// live records arriving during the backfill are buffered
	sendRecord(t, conn, 7, "g")
	sendRecord(t, conn, 8, "h")
	close(releaseFetch)

	for i, data := range []string{"c", "d", "e", "f", "g", "h"} {
		expectRecord(t, source.Out(), uint64(i+3), data)
	}
}

func TestSequencedSource_OverlappingBackfillDeduplicated(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetcher := &recordingFetcher{
		records: map[uint64]string{2: "b", 3: "c", 4: "d"},
		hook: func(ctx context.Context) error {
			close(fetchStarted)
			select {
			case <-releaseFetch:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 5, "e")
	expectRecord(t, source.Out(), 1, "a")
	<-fetchStarted
	// the live record overlaps the requested range with identical content
	sendRecord(t, conn, 4, "d")
	close(releaseFetch)

	for i, data := range []string{"b", "c", "d", "e"} {
		expectRecord(t, source.Out(), uint64(i+2), data)
	}
}

func TestSequencedSource_IncompleteBackfill(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// record 3 is missing from the history
	fetcher := &recordingFetcher{records: map[uint64]string{2: "b"}}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 4, "d")

	expectRecord(t, source.Out(), 1, "a")
	expectClosed(t, source.Out())
	if err := source.Err(); !errors.Is(err, ErrIncompleteBackfill) {
		t.Fatalf("expected ErrIncompleteBackfill, got %v", err)
	}
}

func TestSequencedSource_ConflictingBufferedRecord(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &recordingFetcher{
		hook: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 5, "x")
	sendRecord(t, conn, 5, "y") // same sequence number, different content

	expectRecord(t, source.Out(), 1, "a")
	expectClosed(t, source.Out())
	if err := source.Err(); !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("expected ErrSequenceConflict, got %v", err)
	}
}

func TestSequencedSource_ConflictingBackfillRecord(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetcher := &recordingFetcher{
		records: map[uint64]string{2: "b", 3: "c", 4: "d", 5: "history"},
		hook: func(ctx context.Context) error {
			close(fetchStarted)
			select {
			case <-releaseFetch:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	// signal when the run loop is processing the live conflicting record,
	// so it is guaranteed to be buffered before the backfill is released
	liveProcessed := make(chan struct{})
	config := testConfig(fetcher)
	extract := config.ExtractSequence
	config.ExtractSequence = func(msg Message) (uint64, error) {
		seq, err := extract(msg)
		if err == nil && seq == 5 && strings.Contains(string(msg.Payload), "live") {
			close(liveProcessed)
		}
		return seq, err
	}
	source := newTestSource(t, ctx, server.url(), config)
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 6, "f")
	expectRecord(t, source.Out(), 1, "a")
	<-fetchStarted
	// the live record overlaps the requested range with conflicting content
	sendRecord(t, conn, 5, "live")
	<-liveProcessed
	close(releaseFetch)

	expectClosed(t, source.Out())
	if err := source.Err(); !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("expected ErrSequenceConflict, got %v", err)
	}
}

func TestSequencedSource_ConflictingRedelivery(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newTestSource(t, ctx, server.url(), testConfig(&recordingFetcher{}))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 1, "b") // conflicting retransmission of the last record

	expectRecord(t, source.Out(), 1, "a")
	expectClosed(t, source.Out())
	if err := source.Err(); !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("expected ErrSequenceConflict, got %v", err)
	}
}

func TestSequencedSource_BufferOverflow(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := testConfig(&recordingFetcher{
		hook: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	config.BufferCapacity = 2
	source := newTestSource(t, ctx, server.url(), config)
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 10, "x")
	sendRecord(t, conn, 11, "y")
	sendRecord(t, conn, 12, "z") // exceeds the buffer capacity

	expectRecord(t, source.Out(), 1, "a")
	expectClosed(t, source.Out())
	if err := source.Err(); !errors.Is(err, ErrBufferOverflow) {
		t.Fatalf("expected ErrBufferOverflow, got %v", err)
	}
}

func TestSequencedSource_ReconnectAndBackfill(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var connects int
	config := testConfig(&recordingFetcher{records: map[uint64]string{3: "c", 4: "d"}})
	config.OnConnect = func(ctx context.Context, conn *ws.Conn) error {
		mu.Lock()
		defer mu.Unlock()
		connects++
		return nil
	}
	source := newTestSource(t, ctx, server.url(), config)
	conn1 := server.conn(t)

	sendRecord(t, conn1, 1, "a")
	sendRecord(t, conn1, 2, "b")
	expectRecord(t, source.Out(), 1, "a")
	expectRecord(t, source.Out(), 2, "b")

	// simulate a connection loss; the source should reconnect
	if err := conn1.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
	conn2 := server.conn(t)
	sendRecord(t, conn2, 5, "e")

	for i, data := range []string{"c", "d", "e"} {
		expectRecord(t, source.Out(), uint64(i+3), data)
	}
	mu.Lock()
	if connects != 2 {
		t.Fatalf("expected 2 OnConnect invocations, got %d", connects)
	}
	mu.Unlock()
}

func TestSequencedSource_ReconnectExhausted(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := testConfig(&recordingFetcher{})
	config.MaxReconnects = 2
	source := newTestSource(t, ctx, server.url(), config)
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	expectRecord(t, source.Out(), 1, "a")

	// close the connection and shut down the server so reconnects fail
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
	server.close()

	expectClosed(t, source.Out())
	err := source.Err()
	if err == nil || !strings.Contains(err.Error(), "reconnect attempts exhausted") {
		t.Fatalf("expected reconnect exhaustion error, got %v", err)
	}
}

func TestSequencedSource_CancelAbortsBackfill(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	fetchStarted := make(chan struct{})
	fetchAborted := make(chan struct{})
	fetcher := &recordingFetcher{
		hook: func(ctx context.Context) error {
			close(fetchStarted)
			<-ctx.Done()
			close(fetchAborted)
			return ctx.Err()
		},
	}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 5, "e")
	expectRecord(t, source.Out(), 1, "a")
	<-fetchStarted // wait for the backfill request to be in flight

	cancel()
	<-fetchAborted // the in-flight backfill request was canceled
	expectClosed(t, source.Out())
	if err := source.Err(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// the connection should be closed by the source
	connClosed := make(chan struct{})
	go func() {
		defer close(connClosed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	select {
	case <-connClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed")
	}
}

func TestSequencedSource_StartSequence(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &recordingFetcher{records: map[uint64]string{10: "j", 11: "k"}}
	config := testConfig(fetcher)
	config.StartSequence = 10
	source := newTestSource(t, ctx, server.url(), config)
	conn := server.conn(t)

	// records below the start sequence are ignored
	sendRecord(t, conn, 8, "h")
	sendRecord(t, conn, 12, "l")

	expectRecord(t, source.Out(), 10, "j")
	expectRecord(t, source.Out(), 11, "k")
	expectRecord(t, source.Out(), 12, "l")
	ranges := fetcher.requestedRanges()
	if len(ranges) != 1 || ranges[0] != [2]uint64{10, 11} {
		t.Fatalf("expected backfill range [10, 11], got %v", ranges)
	}
}

func TestSequencedSource_ExtractError(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newTestSource(t, ctx, server.url(), testConfig(&recordingFetcher{}))
	conn := server.conn(t)

	sendRaw(t, conn, "not-json")

	expectClosed(t, source.Out())
	err := source.Err()
	if err == nil || !strings.Contains(err.Error(), "cannot extract sequence number") {
		t.Fatalf("expected sequence extraction error, got %v", err)
	}
}

func TestSequencedSource_Via(t *testing.T) {
	server := newWSTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &recordingFetcher{records: map[uint64]string{2: "b"}}
	source := newTestSource(t, ctx, server.url(), testConfig(fetcher))
	conn := server.conn(t)

	// stream the records through a Map operator
	mapped := source.Via(flow.NewMap(func(msg Message) Message {
		return msg
	}, 1))

	sendRecord(t, conn, 1, "a")
	sendRecord(t, conn, 3, "c")

	expectRecord(t, mapped.Out(), 1, "a")
	expectRecord(t, mapped.Out(), 2, "b")
	expectRecord(t, mapped.Out(), 3, "c")
}

func TestSequencedSource_ConfigValidation(t *testing.T) {
	server := newWSTestServer(t)
	ctx := context.Background()

	valid := testConfig(&recordingFetcher{})
	tests := []struct {
		name   string
		mutate func(*SequencedSourceConfig)
	}{
		{"nil ExtractSequence", func(c *SequencedSourceConfig) { c.ExtractSequence = nil }},
		{"nil HistoryFetcher", func(c *SequencedSourceConfig) { c.HistoryFetcher = nil }},
		{"negative BufferCapacity", func(c *SequencedSourceConfig) { c.BufferCapacity = -1 }},
		{"negative MaxReconnects", func(c *SequencedSourceConfig) { c.MaxReconnects = -1 }},
		{"negative ReconnectDelay", func(c *SequencedSourceConfig) { c.ReconnectDelay = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := *valid
			tt.mutate(&config)
			if _, err := NewSequencedSource(ctx, server.url(), &config); err == nil {
				t.Fatal("expected config validation error")
			}
		})
	}

	if _, err := NewSequencedSource(ctx, server.url(), nil); err == nil {
		t.Fatal("expected error for nil config")
	}
}
