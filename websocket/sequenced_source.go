package websocket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ErrSkipMessage can be returned by SequencedSourceConfig.ExtractSequence to
// indicate that a message carries no sequence number and should be dropped,
// e.g. heartbeats or subscription acknowledgments.
var ErrSkipMessage = errors.New("websocket: message carries no sequence number")

// Terminal failure modes of SequencedSource; test for them using errors.Is.
var (
	// ErrSequenceConflict indicates records with equal sequence numbers but
	// different content.
	ErrSequenceConflict = errors.New("websocket: conflicting record content")
	// ErrBufferOverflow indicates that the live-record buffer capacity was
	// exceeded while a sequence gap was being backfilled.
	ErrBufferOverflow = errors.New("websocket: live-record buffer overflow")
	// ErrIncompleteBackfill indicates that the history fetcher did not return
	// the complete requested sequence range.
	ErrIncompleteBackfill = errors.New("websocket: incomplete backfill")
)

const (
	// defaultBufferCapacity is the default live-record buffer capacity.
	defaultBufferCapacity = 1024
	// defaultMaxReconnects is the default number of consecutive connection
	// recovery attempts before the source fails.
	defaultMaxReconnects = 3
)

// SequencedSourceConfig configures a SequencedSource.
type SequencedSourceConfig struct {
	// ExtractSequence returns the sequence number of a received message.
	// Return ErrSkipMessage to silently drop messages that carry no sequence
	// number, such as heartbeats. Any other error is terminal.
	// Required.
	ExtractSequence func(msg Message) (uint64, error)

	// HistoryFetcher retrieves historical records to backfill sequence gaps.
	// The records it returns must have the same content representation as the
	// live records. Required.
	HistoryFetcher HistoryFetcher

	// StartSequence is the sequence number expected for the first record.
	// If zero, the first received record establishes the sequence baseline.
	StartSequence uint64

	// BufferCapacity bounds the number of live records buffered while a
	// sequence gap is being backfilled. Exceeding it is a terminal failure.
	// Default config value: 1024.
	BufferCapacity int

	// MaxReconnects bounds the number of consecutive connection recovery
	// attempts before the source fails. The counter is reset once a message
	// has been read successfully. Default config value: 3.
	MaxReconnects int

	// ReconnectDelay is the delay between connection recovery attempts.
	// Default config value: 0.
	ReconnectDelay time.Duration

	// OnConnect is invoked after every (re)connection, e.g. to (re)subscribe
	// to the data feed. A non-nil error is terminal on the initial connection
	// and is retried during connection recovery.
	OnConnect func(ctx context.Context, conn *ws.Conn) error

	// Logger is the logger for the source. If nil, slog.Default() is used.
	Logger *slog.Logger
}

func (c *SequencedSourceConfig) validate() error {
	if c.ExtractSequence == nil {
		return errors.New("ExtractSequence is required")
	}
	if c.HistoryFetcher == nil {
		return errors.New("HistoryFetcher is required")
	}
	if c.BufferCapacity < 0 {
		return errors.New("BufferCapacity must not be negative")
	}
	if c.BufferCapacity == 0 {
		c.BufferCapacity = defaultBufferCapacity
	}
	if c.MaxReconnects < 0 {
		return errors.New("MaxReconnects must not be negative")
	}
	if c.MaxReconnects == 0 {
		c.MaxReconnects = defaultMaxReconnects
	}
	if c.ReconnectDelay < 0 {
		return errors.New("ReconnectDelay must not be negative")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// SequencedSource represents a WebSocket source connector that delivers
// strictly ordered, gap-free business records carrying consecutive sequence
// numbers. Duplicate records are ignored. When a sequence gap is detected,
// live records are buffered (up to a bounded capacity) and the missing range
// is backfilled using the configured HistoryFetcher before delivery resumes.
// Incomplete backfills, conflicting record content and buffer overflows are
// terminal failures reported via Err; sequence numbers are never skipped
// silently. Canceling the context closes the connection and aborts any
// in-flight backfill request.
type SequencedSource struct {
	config SequencedSourceConfig
	url    string
	dialer *ws.Dialer

	ctx    context.Context
	cancel context.CancelFunc

	conn *ws.Conn // accessed by the run goroutine only
	out  chan any

	errMu sync.Mutex
	err   error

	logger *slog.Logger
}

var _ streams.Source = (*SequencedSource)(nil)

// NewSequencedSource creates and returns a new [SequencedSource] using the
// default dialer.
func NewSequencedSource(ctx context.Context, url string,
	config *SequencedSourceConfig) (*SequencedSource, error) {
	return NewSequencedSourceWithDialer(ctx, url, ws.DefaultDialer, config)
}

// NewSequencedSourceWithDialer returns a new [SequencedSource] using the
// provided dialer.
func NewSequencedSourceWithDialer(ctx context.Context, url string, dialer *ws.Dialer,
	config *SequencedSourceConfig) (*SequencedSource, error) {
	if config == nil {
		return nil, errors.New("websocket: config is required")
	}
	cfg := *config
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("websocket: invalid sequenced source config: %w", err)
	}
	cfg.Logger = cfg.Logger.With(slog.Group("connector",
		slog.String("name", "websocket"),
		slog.String("type", "sequenced-source")))

	ctx, cancel := context.WithCancel(ctx)
	source := &SequencedSource{
		config: cfg,
		url:    url,
		dialer: dialer,
		ctx:    ctx,
		cancel: cancel,
		out:    make(chan any),
		logger: cfg.Logger,
	}

	// create the initial client connection
	conn, err := source.dial(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	source.conn = conn

	// asynchronously consume data and send it downstream in sequence order
	go source.run()

	return source, nil
}

// dial establishes a new connection and invokes the OnConnect hook.
func (s *SequencedSource) dial(ctx context.Context) (*ws.Conn, error) {
	conn, _, err := s.dialer.DialContext(ctx, s.url, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket: failed to dial: %w", err)
	}
	if onConnect := s.config.OnConnect; onConnect != nil {
		if err := onConnect(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("websocket: on-connect hook failed: %w", err)
		}
	}
	return conn, nil
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *SequencedSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the SequencedSource connector.
func (s *SequencedSource) Out() <-chan any {
	return s.out
}

// Err returns the terminal error that caused the source to stop, if any.
// It should be called after the Out channel has been closed. A nil error
// indicates a clean shutdown following context cancellation.
func (s *SequencedSource) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// fail records the terminal error and logs it.
func (s *SequencedSource) fail(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
	s.logger.Error("Terminating connector", slog.Any("error", err))
}

// closeConn closes the current connection, unblocking the read pump.
func (s *SequencedSource) closeConn() {
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			s.logger.Warn("Error in connection.Close", slog.Any("error", err))
		}
	}
}

// rawEvent is a message or a terminal read error from the read pump.
type rawEvent struct {
	msg Message
	err error
}

// fetchResult is the outcome of a single backfill request.
type fetchResult struct {
	from, to uint64
	messages []Message
	err      error
}

// runState holds the mutable state of the run loop.
type runState struct {
	nextSeq    uint64
	baseline   bool // whether nextSeq has been established
	buffer     map[uint64]Message
	lastSeq    uint64
	lastMsg    Message
	haveLast   bool
	reconnects int
}

// runLoop orchestrates the read pump, backfill requests and ordered delivery.
type runLoop struct {
	source  *SequencedSource
	state   runState
	rawCh   chan rawEvent
	fetchCh chan fetchResult // nil while no backfill request is in flight
}

// run is the main loop of the connector.
func (s *SequencedSource) run() {
	defer close(s.out)
	defer s.cancel()    // abort an in-flight backfill request
	defer s.closeConn() // unblock the read pump

	rl := &runLoop{
		source: s,
		state: runState{
			nextSeq:  s.config.StartSequence,
			baseline: s.config.StartSequence > 0,
			buffer:   make(map[uint64]Message),
		},
		rawCh: make(chan rawEvent),
	}
	rl.startPump()

	for {
		select {
		case event := <-rl.rawCh:
			if !rl.handleRawEvent(event) {
				return
			}
		case result := <-rl.fetchCh:
			if !rl.handleFetchResult(result) {
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// startPump launches a read pump for the current connection.
func (rl *runLoop) startPump() {
	conn := rl.source.conn
	go func() {
		for {
			msgType, payload, err := conn.ReadMessage()
			event := rawEvent{msg: Message{MsgType: msgType, Payload: payload}, err: err}
			select {
			case rl.rawCh <- event:
			case <-rl.source.ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
}

// handleRawEvent processes one event from the read pump. It returns false if
// the run loop should terminate.
func (rl *runLoop) handleRawEvent(event rawEvent) bool {
	if event.err != nil {
		return rl.recoverConnection(event.err)
	}
	rl.state.reconnects = 0 // the connection has proven healthy

	seq, err := rl.source.config.ExtractSequence(event.msg)
	if errors.Is(err, ErrSkipMessage) {
		return true
	}
	if err != nil {
		rl.source.fail(fmt.Errorf("websocket: cannot extract sequence number: %w", err))
		return false
	}

	if !rl.state.baseline {
		rl.state.baseline = true
		rl.state.nextSeq = seq
	}
	switch {
	case seq < rl.state.nextSeq:
		// a retransmission of an already delivered record
		return rl.checkRedelivery(seq, event.msg)
	case rl.fetchCh != nil || seq > rl.state.nextSeq:
		// buffer live records while backfilling, or on a sequence gap
		if !rl.bufferRecord(seq, event.msg) {
			return false
		}
		if rl.fetchCh == nil {
			rl.startFetch(rl.state.nextSeq, seq-1)
		}
		return true
	default:
		return rl.deliver(seq, event.msg)
	}
}

// checkRedelivery verifies a duplicate record against the last delivered one.
func (rl *runLoop) checkRedelivery(seq uint64, msg Message) bool {
	if rl.state.haveLast && seq == rl.state.lastSeq && !messagesEqual(msg, rl.state.lastMsg) {
		rl.source.fail(fmt.Errorf("%w: sequence number %d", ErrSequenceConflict, seq))
		return false
	}
	rl.source.logger.Debug("Ignoring duplicate record", slog.Uint64("seq", seq))
	return true
}

// bufferRecord stores a live record in the bounded gap buffer.
func (rl *runLoop) bufferRecord(seq uint64, msg Message) bool {
	if buffered, ok := rl.state.buffer[seq]; ok {
		if !messagesEqual(buffered, msg) {
			rl.source.fail(fmt.Errorf("%w: sequence number %d", ErrSequenceConflict, seq))
			return false
		}
		return true // identical retransmission
	}
	if len(rl.state.buffer) >= rl.source.config.BufferCapacity {
		rl.source.fail(fmt.Errorf("%w: capacity %d", ErrBufferOverflow,
			rl.source.config.BufferCapacity))
		return false
	}
	rl.state.buffer[seq] = msg
	return true
}

// startFetch launches a backfill request for the inclusive range [from, to].
func (rl *runLoop) startFetch(from, to uint64) {
	fetchCh := make(chan fetchResult, 1)
	rl.fetchCh = fetchCh
	rl.source.logger.Info("Backfilling sequence gap",
		slog.Uint64("from", from), slog.Uint64("to", to))
	go func() {
		messages, err := rl.source.config.HistoryFetcher.Fetch(rl.source.ctx, from, to)
		fetchCh <- fetchResult{from: from, to: to, messages: messages, err: err}
	}()
}

// handleFetchResult processes a completed backfill request. It returns false
// if the run loop should terminate.
func (rl *runLoop) handleFetchResult(result fetchResult) bool {
	rl.fetchCh = nil
	if result.err != nil {
		if rl.source.ctx.Err() != nil {
			return false // the request was aborted by cancellation
		}
		rl.source.fail(fmt.Errorf("websocket: backfill request failed: %w", result.err))
		return false
	}
	fetched, ok := rl.validateBackfill(result)
	if !ok {
		return false
	}
	// deliver the fetched range in sequence order
	for seq := result.from; ; seq++ {
		if !rl.deliver(seq, fetched[seq]) {
			return false
		}
		if seq == result.to {
			break
		}
	}
	// drain consecutively buffered live records
	for {
		msg, ok := rl.state.buffer[rl.state.nextSeq]
		if !ok {
			break
		}
		delete(rl.state.buffer, rl.state.nextSeq)
		if !rl.deliver(rl.state.nextSeq, msg) {
			return false
		}
	}
	// backfill the next missing range if a gap remains
	if len(rl.state.buffer) > 0 {
		rl.startFetch(rl.state.nextSeq, minBufferedSeq(rl.state.buffer)-1)
	}
	return true
}

// validateBackfill verifies that the fetched records completely cover the
// requested range and do not conflict with buffered live records.
func (rl *runLoop) validateBackfill(result fetchResult) (map[uint64]Message, bool) {
	fetched := make(map[uint64]Message, len(result.messages))
	for _, msg := range result.messages {
		seq, err := rl.source.config.ExtractSequence(msg)
		if err != nil {
			rl.source.fail(fmt.Errorf("websocket: cannot extract sequence number "+
				"of a backfilled record: %w", err))
			return nil, false
		}
		if seq < result.from || seq > result.to {
			rl.source.fail(fmt.Errorf("%w: record %d outside the requested range [%d, %d]",
				ErrIncompleteBackfill, seq, result.from, result.to))
			return nil, false
		}
		if existing, ok := fetched[seq]; ok && !messagesEqual(existing, msg) {
			rl.source.fail(fmt.Errorf("%w: sequence number %d", ErrSequenceConflict, seq))
			return nil, false
		}
		fetched[seq] = msg
	}
	for seq := result.from; ; seq++ {
		msg, ok := fetched[seq]
		if !ok {
			rl.source.fail(fmt.Errorf("%w: missing record %d", ErrIncompleteBackfill, seq))
			return nil, false
		}
		if buffered, ok := rl.state.buffer[seq]; ok {
			if !messagesEqual(buffered, msg) {
				rl.source.fail(fmt.Errorf("%w: sequence number %d", ErrSequenceConflict, seq))
				return nil, false
			}
			delete(rl.state.buffer, seq)
		}
		if seq == result.to {
			break
		}
	}
	return fetched, true
}

// deliver sends a record downstream, updating the sequence bookkeeping.
func (rl *runLoop) deliver(seq uint64, msg Message) bool {
	select {
	case rl.source.out <- msg:
		rl.state.lastSeq, rl.state.lastMsg, rl.state.haveLast = seq, msg, true
		rl.state.nextSeq = seq + 1
		return true
	case <-rl.source.ctx.Done():
		return false
	}
}

// recoverConnection reestablishes a lost connection with a bounded number of
// attempts. It returns false if the run loop should terminate.
func (rl *runLoop) recoverConnection(readErr error) bool {
	s := rl.source
	s.closeConn()
	for {
		rl.state.reconnects++
		if rl.state.reconnects > s.config.MaxReconnects {
			s.fail(fmt.Errorf("websocket: connection lost and reconnect attempts "+
				"exhausted: %w", readErr))
			return false
		}
		s.logger.Warn("Connection lost; reconnecting", slog.Any("error", readErr),
			slog.Int("attempt", rl.state.reconnects))
		if !sleepContext(s.ctx, s.config.ReconnectDelay) {
			return false
		}
		conn, err := s.dial(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return false
			}
			s.logger.Warn("Reconnect attempt failed", slog.Any("error", err))
			continue
		}
		s.conn = conn
		rl.startPump()
		return true
	}
}

// messagesEqual compares two messages by type and payload content.
func messagesEqual(a, b Message) bool {
	return a.MsgType == b.MsgType && bytes.Equal(a.Payload, b.Payload)
}

// minBufferedSeq returns the smallest sequence number in the buffer.
func minBufferedSeq(buffer map[uint64]Message) uint64 {
	minSeq := uint64(math.MaxUint64)
	for seq := range buffer {
		if seq < minSeq {
			minSeq = seq
		}
	}
	return minSeq
}

// sleepContext sleeps for the specified duration, returning false if the
// context is canceled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
