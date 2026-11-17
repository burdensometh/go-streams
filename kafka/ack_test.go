package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams/flow"
)

// fakeSession is a sarama.ConsumerGroupSession implementation that records the
// marked offsets.
type fakeSession struct {
	ctx context.Context

	mu      sync.Mutex
	offsets map[partitionKey]int64
	commits int
}

var _ sarama.ConsumerGroupSession = (*fakeSession)(nil)

func newFakeSession(ctx context.Context) *fakeSession {
	return &fakeSession{
		ctx:     ctx,
		offsets: make(map[partitionKey]int64),
	}
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "test" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) Context() context.Context                 { return s.ctx }

func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partitionKey{topic: topic, partition: partition}] = offset
}

func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, metadata string) {
	s.MarkOffset(message.Topic, message.Partition, message.Offset+1, metadata)
}

func (s *fakeSession) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
}

func (s *fakeSession) marked(topic string, partition int32) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset, ok := s.offsets[partitionKey{topic: topic, partition: partition}]
	return offset, ok
}

func (s *fakeSession) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

// fakeClaim is a sarama.ConsumerGroupClaim implementation backed by a channel.
type fakeClaim struct {
	topic     string
	partition int32
	initial   int64
	messages  chan *sarama.ConsumerMessage
}

var _ sarama.ConsumerGroupClaim = (*fakeClaim)(nil)

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return c.partition }
func (c *fakeClaim) InitialOffset() int64                     { return c.initial }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// message returns a test message with the given offset.
func message(partition int32, offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: partition,
		Offset:    offset,
		Value:     []byte("value"),
	}
}

// testLogger returns a logger discarding its output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestTracker returns a session tracker for a fake session.
func newTestTracker(t *testing.T) (*sessionTracker, *fakeSession) {
	t.Helper()
	session := newFakeSession(context.Background())
	tracker := newSessionTracker(session, testLogger(), time.Hour)
	t.Cleanup(tracker.close)
	return tracker, session
}

func TestRecordAckAdvancesContiguousOffsets(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	for offset := int64(10); offset < 14; offset++ {
		partition.deliver(offset)
	}

	// out of order acknowledgments; the offset may only advance once the
	// preceding record is completed.
	partition.complete(12)
	assertNotMarked(t, session)
	partition.complete(10)
	assertMarked(t, session, 11)
	partition.complete(13)
	assertMarked(t, session, 11)

	// completing the missing record releases all the subsequent ones
	partition.complete(11)
	assertMarked(t, session, 14)
}

func TestRecordAckDoesNotOvertakeFailure(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	for offset := int64(0); offset < 4; offset++ {
		partition.deliver(offset)
	}

	// the first record fails; later successful records must not advance the
	// partition offset past it.
	for offset := int64(1); offset < 4; offset++ {
		partition.complete(offset)
	}
	assertNotMarked(t, session)

	// a late completion of the failed record releases the chain
	partition.complete(0)
	assertMarked(t, session, 4)
}

func TestRecordDiscardAcknowledgement(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	record := &Record{
		ConsumerMessage: message(0, 5),
		partition:       partition,
		offset:          5,
	}
	partition.deliver(record.offset)

	record.Discard()
	assertMarked(t, session, 6)
}

func TestRecordFailBlocksPartition(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	failed := &Record{
		ConsumerMessage: message(0, 0),
		partition:       partition,
		offset:          0,
	}
	next := &Record{
		ConsumerMessage: message(0, 1),
		partition:       partition,
		offset:          1,
	}
	partition.deliver(failed.offset)
	partition.deliver(next.offset)

	failed.Fail(errors.New("write failed"))
	next.Ack()

	assertNotMarked(t, session)
}

func TestRecordFailDoesNotConsumeAcknowledgement(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	record := &Record{
		ConsumerMessage: message(0, 0),
		partition:       partition,
		offset:          0,
	}
	partition.deliver(0)

	record.Fail(errors.New("transient failure"))
	assertNotMarked(t, session)

	// a later retry acknowledges the record and advances the offset
	record.Ack()
	assertMarked(t, session, 1)
}

func TestRecordDuplicateAckIgnored(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)
	partition.deliver(0)
	partition.deliver(1)

	first := &Record{ConsumerMessage: message(0, 0), partition: partition, offset: 0}
	second := &Record{ConsumerMessage: message(0, 1), partition: partition, offset: 1}

	first.Ack()
	first.Ack()
	second.Ack()

	eqInt64(t, mustMarked(t, session, 0), 2)
}

func TestSkippedOffsetsAreSkipped(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	partition.deliver(0)
	partition.deliver(5)
	partition.complete(0)

	// offsets 1-4 were not delivered in this session; the next committable
	// offset is 5, which is still outstanding.
	assertMarked(t, session, 5)

	partition.complete(5)
	assertMarked(t, session, 6)
}

func TestLateAckDoesNotAdvanceOldSession(t *testing.T) {
	oldSession := newFakeSession(context.Background())
	oldTracker := newSessionTracker(oldSession, testLogger(), time.Hour)
	oldPartition := oldTracker.partition("test", 0)
	oldPartition.deliver(0)
	oldPartition.deliver(1)
	oldPartition.complete(0)
	eqInt64(t, mustMarked(t, oldSession, 0), 1)

	// the session ends; the record at offset 1 was not completed
	oldTracker.close()
	oldOffset := mustMarked(t, oldSession, 0)
	oldCommits := oldSession.commitCount()

	// a new session starts at the last committed offset
	newSession := newFakeSession(context.Background())
	newTracker := newSessionTracker(newSession, testLogger(), time.Hour)
	defer newTracker.close()
	newPartition := newTracker.partition("test", 0)
	newPartition.deliver(1)
	newPartition.complete(1)
	eqInt64(t, mustMarked(t, newSession, 0), 2)

	// the late acknowledgment arrives after the rebalance and must not
	// advance either session.
	oldPartition.complete(1)

	eqInt64(t, mustMarked(t, oldSession, 0), oldOffset)
	eqInt(t, oldSession.commitCount(), oldCommits)
	eqInt64(t, mustMarked(t, newSession, 0), 2)
}

func TestCancelKeepsIncompleteRecordsForRedelivery(t *testing.T) {
	session := newFakeSession(context.Background())
	tracker := newSessionTracker(session, testLogger(), time.Hour)

	partition := tracker.partition("test", 0)
	partition.deliver(20)
	partition.deliver(21)
	partition.complete(20)

	// the consumer is cancelled; only the completed offsets are committed
	tracker.close()

	eqInt64(t, mustMarked(t, session, 0), 21)
	eqInt(t, session.commitCount(), 1)
}

func TestConcurrentAcks(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	const count = 100
	for offset := int64(0); offset < count; offset++ {
		partition.deliver(offset)
	}

	var wg sync.WaitGroup
	for offset := int64(0); offset < count; offset++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			partition.complete(offset)
		}(offset)
	}
	wg.Wait()

	eqInt64(t, mustMarked(t, session, 0), count)
}

func TestCommitLoopCommitsMarkedOffsets(t *testing.T) {
	session := newFakeSession(context.Background())
	tracker := newSessionTracker(session, testLogger(), 5*time.Millisecond)
	defer tracker.close()

	partition := tracker.partition("test", 0)
	partition.deliver(0)
	partition.complete(0)

	deadline := time.Now().Add(time.Second)
	for session.commitCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("expected a periodic commit")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSourceEmitsRecordsInAckMode(t *testing.T) {
	handler := &groupHandler{
		ready:  make(chan struct{}),
		out:    make(chan any),
		logger: testLogger(),
		opts:   sourceOptions{ackTracking: true, commitInterval: time.Hour},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := newFakeSession(ctx)
	if err := handler.Setup(session); err != nil {
		t.Fatal(err)
	}

	claim := &fakeClaim{
		topic:     "test",
		partition: 0,
		initial:   0,
		messages:  make(chan *sarama.ConsumerMessage, 2),
	}
	claim.messages <- message(0, 0)
	claim.messages <- message(0, 1)
	close(claim.messages)

	done := make(chan error, 1)
	go func() { done <- handler.ConsumeClaim(session, claim) }()

	first := (<-handler.out).(*Record)
	second := (<-handler.out).(*Record)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// the offsets must not be marked while the records are unacknowledged
	assertNotMarked(t, session)

	second.Ack()
	assertNotMarked(t, session)

	first.Ack()
	eqInt64(t, mustMarked(t, session, 0), 2)

	if err := handler.Cleanup(session); err != nil {
		t.Fatal(err)
	}
}

func TestSourceWithoutAckTrackingMarksImmediately(t *testing.T) {
	handler := &groupHandler{
		ready:  make(chan struct{}),
		out:    make(chan any),
		logger: testLogger(),
		opts:   makeDefaultSourceOptions(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := newFakeSession(ctx)

	claim := &fakeClaim{
		topic:     "test",
		partition: 0,
		initial:   0,
		messages:  make(chan *sarama.ConsumerMessage, 1),
	}
	claim.messages <- message(0, 0)
	close(claim.messages)

	done := make(chan error, 1)
	go func() { done <- handler.ConsumeClaim(session, claim) }()

	element := <-handler.out
	if _, ok := element.(*sarama.ConsumerMessage); !ok {
		t.Fatalf("expected *sarama.ConsumerMessage, got %T", element)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	eqInt64(t, mustMarked(t, session, 0), 1)
}

func TestConsumeClaimReturnsWhenMessagesClosed(t *testing.T) {
	handler := &groupHandler{
		ready:  make(chan struct{}),
		out:    make(chan any),
		logger: testLogger(),
		opts:   makeDefaultSourceOptions(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := newFakeSession(ctx)

	claim := &fakeClaim{topic: "test", messages: make(chan *sarama.ConsumerMessage)}
	close(claim.messages)

	done := make(chan error, 1)
	go func() { done <- handler.ConsumeClaim(session, claim) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("ConsumeClaim did not return after the messages channel was closed")
	}
}

func TestRecords(t *testing.T) {
	record := &Record{ConsumerMessage: message(0, 0)}

	eqInt(t, len(Records(record)), 1)
	eqInt(t, len(Records([]*Record{record, nil})), 1)
	eqInt(t, len(Records([]any{record, []*Record{record}})), 2)
	eqInt(t, len(Records("value")), 0)
	eqInt(t, len(Records(nil)), 0)
}

// stubProducer is a sarama.SyncProducer that writes the received messages to
// the given slice and fails according to the configured number of messages.
type stubProducer struct {
	mu       sync.Mutex
	sent     []*sarama.ProducerMessage
	failures int
}

var _ sarama.SyncProducer = (*stubProducer)(nil)

func (p *stubProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return -1, -1, errors.New("produce failed")
	}
	p.sent = append(p.sent, message)
	return 0, int64(len(p.sent)), nil
}

func (p *stubProducer) SendMessages(messages []*sarama.ProducerMessage) error {
	for _, message := range messages {
		if _, _, err := p.SendMessage(message); err != nil {
			return err
		}
	}
	return nil
}

func (p *stubProducer) Close() error                            { return nil }
func (p *stubProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (p *stubProducer) IsTransactional() bool                   { return false }
func (p *stubProducer) BeginTxn() error                         { return nil }
func (p *stubProducer) CommitTxn() error                        { return nil }
func (p *stubProducer) AbortTxn() error                         { return nil }
func (p *stubProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata,
	string) error {
	return nil
}
func (p *stubProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func (p *stubProducer) sentCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sent)
}

func TestSinkAcknowledgesWrittenRecords(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	producer := &stubProducer{}
	sink := NewSaramaSink(producer, "target", testLogger())

	record := &Record{
		ConsumerMessage: message(0, 0),
		partition:       partition,
		offset:          0,
	}
	partition.deliver(0)

	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()

	assertMarked(t, session, 1)
	eqInt(t, producer.sentCount(), 1)
}

func TestSinkReportsFailedWrite(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	producer := &stubProducer{failures: 1}
	sink := NewSaramaSink(producer, "target", testLogger())

	first := &Record{ConsumerMessage: message(0, 0), partition: partition, offset: 0}
	second := &Record{ConsumerMessage: message(0, 1), partition: partition, offset: 1}
	partition.deliver(0)
	partition.deliver(1)

	sink.In() <- first
	sink.In() <- second
	close(sink.In())
	sink.AwaitCompletion()

	// the second record was written, but the first one failed, hence the
	// partition offset must not advance past it.
	assertNotMarked(t, session)
	eqInt(t, producer.sentCount(), 1)
}

func TestSinkAcknowledgesRecordsInBatch(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	producer := &stubProducer{}
	sink := NewSaramaSink(producer, "target", testLogger())

	first := &Record{ConsumerMessage: message(0, 0), partition: partition, offset: 0}
	second := &Record{ConsumerMessage: message(0, 1), partition: partition, offset: 1}
	partition.deliver(0)
	partition.deliver(1)

	sink.In() <- []*Record{first, second}
	close(sink.In())
	sink.AwaitCompletion()

	assertMarked(t, session, 2)
	eqInt(t, producer.sentCount(), 2)
}

func TestSinkProcessesPlainMessages(t *testing.T) {
	producer := &stubProducer{}
	sink := NewSaramaSink(producer, "target", testLogger())

	sink.In() <- message(0, 0)
	sink.In() <- "value"
	sink.In() <- &sarama.ProducerMessage{
		Topic: "target",
		Value: sarama.StringEncoder("value"),
	}
	close(sink.In())
	sink.AwaitCompletion()

	eqInt(t, producer.sentCount(), 3)
}

func TestPipelineAdvancesOffsetsAfterWrite(t *testing.T) {
	tracker, session := newTestTracker(t)
	partition := tracker.partition("test", 0)

	producer := &stubProducer{}
	sink := NewSaramaSink(producer, "target", testLogger())

	for offset := int64(0); offset < 6; offset++ {
		partition.deliver(offset)
	}

	// process the records in parallel and drop the odd values through a
	// filter; the retained records are written by the sink.
	retain := flow.NewFilter(func(record *Record) bool {
		return record.Offset%2 == 0
	}, 3)
	toUpper := flow.NewMap(func(record *Record) *Record {
		record.Value = []byte(strings.ToUpper(string(record.Value)))
		return record
	}, 3)

	done := make(chan struct{})
	go func() {
		defer close(done)
		retain.Via(toUpper).To(sink)
	}()

	for offset := int64(0); offset < 6; offset++ {
		retain.In() <- &Record{
			ConsumerMessage: message(0, offset),
			partition:       partition,
			offset:          offset,
		}
	}
	close(retain.In())
	<-done

	// every record, including the filtered ones, has been completed
	assertMarked(t, session, 6)
	eqInt(t, producer.sentCount(), 3)
}

func assertNotMarked(t *testing.T, session *fakeSession) {
	t.Helper()
	if offset, ok := session.marked("test", 0); ok {
		t.Fatalf("expected no marked offset, got %d", offset)
	}
}

func assertMarked(t *testing.T, session *fakeSession, expected int64) {
	t.Helper()
	eqInt64(t, mustMarked(t, session, 0), expected)
}

func mustMarked(t *testing.T, session *fakeSession, partition int32) int64 {
	t.Helper()
	offset, ok := session.marked("test", partition)
	if !ok {
		t.Fatalf("expected a marked offset for partition %d", partition)
	}
	return offset
}

func eqInt(t *testing.T, actual, expected int) {
	t.Helper()
	if actual != expected {
		t.Fatalf("expected %d, got %d", expected, actual)
	}
}

func eqInt64(t *testing.T, actual, expected int64) {
	t.Helper()
	if actual != expected {
		t.Fatalf("expected %d, got %d", expected, actual)
	}
}

func TestSourceOptions(t *testing.T) {
	defaults := applySourceOptions()
	eqInt(t, 0, boolToInt(defaults.ackTracking))
	eqInt64(t, int64(defaultCommitInterval), int64(defaults.commitInterval))

	options := applySourceOptions(WithAckTracking(), WithCommitInterval(50*time.Millisecond))
	eqInt(t, 1, boolToInt(options.ackTracking))
	eqInt64(t, int64(50*time.Millisecond), int64(options.commitInterval))

	// non-positive intervals are ignored
	options = applySourceOptions(WithCommitInterval(0), WithCommitInterval(-time.Second))
	eqInt64(t, int64(defaultCommitInterval), int64(options.commitInterval))
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
