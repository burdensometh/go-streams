package kafka

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// defaultCommitInterval is the default interval at which the offsets marked by
// the acknowledgment tracker are committed to the broker.
const defaultCommitInterval = time.Second

// SourceOption is a functional option used to configure a [SaramaSource].
// The option is applied to an opaque configuration that only the package
// knows how to interpret.
type SourceOption func(*sourceOptions)

// sourceOptions holds the optional configuration of a [SaramaSource].
type sourceOptions struct {
	ackTracking    bool
	commitInterval time.Duration
}

// makeDefaultSourceOptions returns sourceOptions with default values.
func makeDefaultSourceOptions() sourceOptions {
	return sourceOptions{
		commitInterval: defaultCommitInterval,
	}
}

// apply applies the options to a configuration.
func applySourceOptions(opts ...SourceOption) sourceOptions {
	options := makeDefaultSourceOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

// WithAckTracking enables the processing completion acknowledgment chain.
//
// When enabled, the source emits [Record] values instead of
// [sarama.ConsumerMessage] values and does not mark the consumed offset until
// the record has been acknowledged. A record is acknowledged when it has been
// successfully written downstream ([Record.Ack]) or explicitly discarded
// ([Record.Discard]). A record that failed to be processed ([Record.Fail]) is
// not acknowledged, so the connector redelivers it after a rebalance or a
// restart.
//
// The offsets are advanced per partition: only the longest sequence of
// consecutively acknowledged records is committed, hence a later successful
// record can never overtake an earlier unfinished one, even when the
// downstream stages process elements in parallel.
//
// Downstream operators must pass the [Record] values through unchanged, or
// acknowledge the records that they consume and drop. The flow.Filter and
// flow.Throttler operators discard elements they intentionally drop when the
// element implements the streams.Discarder interface, which [Record] does, so
// such elements do not stall the partition offset. A flow that replaces a
// record with a different value breaks the acknowledgment chain for that
// record, and its partition offset stops advancing.
//
// The offsets are committed by the connector itself, so the consumer group
// should be configured with automatic commits disabled
// (config.Consumer.Offsets.AutoCommit.Enable = false).
//
// Without this option, the connector keeps its original behavior: messages are
// marked as consumed as soon as they are handed downstream, and no
// acknowledgment is required from the pipeline.
func WithAckTracking() SourceOption {
	return func(o *sourceOptions) {
		o.ackTracking = true
	}
}

// WithCommitInterval sets the interval at which the acknowledgment tracker
// commits the consecutively completed offsets to the broker. The default is
// one second. A non-positive interval is ignored.
//
// The option has an effect only when the source is created with
// [WithAckTracking].
func WithCommitInterval(interval time.Duration) SourceOption {
	return func(o *sourceOptions) {
		if interval > 0 {
			o.commitInterval = interval
		}
	}
}

// Record represents a consumed Kafka record together with its acknowledgment
// state. Records are emitted by a [SaramaSource] created with
// [WithAckTracking].
//
// A Record embeds the underlying [sarama.ConsumerMessage], so the message
// fields are accessible directly. A record is acknowledged once by calling one
// of [Record.Ack] or [Record.Discard]; [Record.Fail] only reports the failure
// and keeps the record unacknowledged. Until the record is acknowledged the
// source does not advance the offset of the record's partition past it, and
// the record is redelivered if the current session ends.
//
// A Record must not be copied after first use.
type Record struct {
	*sarama.ConsumerMessage

	// partition is the tracker of the partition the record was consumed from.
	partition *partitionTracker
	// offset is the offset of the record. It is kept separately from the
	// embedded message, which downstream stages are allowed to modify.
	offset int64

	acknowledged atomic.Bool
}

// Message returns the underlying consumed message.
func (r *Record) Message() *sarama.ConsumerMessage {
	return r.ConsumerMessage
}

// Ack acknowledges that the record has been successfully processed, e.g. it
// has been written to the target system. The partition offset becomes
// committable once all preceding records of the partition are acknowledged.
//
// Repeated calls have no additional effect.
func (r *Record) Ack() {
	if r.partition == nil || !r.acknowledged.CompareAndSwap(false, true) {
		return
	}
	r.partition.complete(r.offset)
}

// Discard acknowledges that the record has been explicitly dropped, e.g. by a
// filtering stage that intentionally discarded it. A discarded record is
// considered fully processed and does not block the partition offset.
//
// Repeated calls have no additional effect.
func (r *Record) Discard() {
	if r.partition == nil || !r.acknowledged.CompareAndSwap(false, true) {
		return
	}
	r.partition.session.logger.Debug("Record discarded",
		slog.String("topic", r.partition.key.topic),
		slog.Int("partition", int(r.partition.key.partition)),
		slog.Int64("offset", r.offset))
	r.partition.complete(r.offset)
}

// Fail reports that the record could not be processed. The record is not
// acknowledged, hence the partition offset does not advance past it and the
// record is redelivered after the current session ends.
//
// A failed record may still be acknowledged later, e.g. when the pipeline
// retries the write and it succeeds. Until then, the subsequent records of the
// partition can not be committed past the failed one.
func (r *Record) Fail(err error) {
	if r.partition == nil || err == nil || r.acknowledged.Load() {
		return
	}
	r.partition.session.logger.Error("Record processing failed",
		slog.String("topic", r.partition.key.topic),
		slog.Int("partition", int(r.partition.key.partition)),
		slog.Int64("offset", r.offset),
		slog.Any("error", err))
}

// Records returns the records carried by a stream element.
//
// The following element types are supported: [*Record], []*Record and []any
// recursively containing the former. The function returns nil for an element
// that does not carry records. It is intended for custom sinks that need to
// acknowledge the processed records.
func Records(element any) []*Record {
	switch value := element.(type) {
	case *Record:
		if value == nil {
			return nil
		}
		return []*Record{value}
	case []*Record:
		records := make([]*Record, 0, len(value))
		for _, record := range value {
			if record != nil {
				records = append(records, record)
			}
		}
		return records
	case []any:
		records := make([]*Record, 0, len(value))
		for _, item := range value {
			records = append(records, Records(item)...)
		}
		return records
	default:
		return nil
	}
}

// partitionKey identifies a topic partition.
type partitionKey struct {
	topic     string
	partition int32
}

// sessionTracker tracks the acknowledgment state of the partition claims of a
// single consumer group session. A tracker is created by the handler Setup
// hook and closed by the Cleanup hook, so an acknowledgment that arrives after
// a rebalance can not advance the offsets of a newer session.
type sessionTracker struct {
	session        sarama.ConsumerGroupSession
	logger         *slog.Logger
	commitInterval time.Duration

	mu         sync.Mutex
	partitions map[partitionKey]*partitionTracker
	closed     bool
	dirty      bool

	// commitMu serializes the offset commits performed by the periodic commit
	// loop and by the final commit on session cleanup.
	commitMu sync.Mutex

	stop chan struct{}
	done chan struct{}
}

// newSessionTracker returns a tracker for the given session and starts the
// periodic offset commit loop.
func newSessionTracker(session sarama.ConsumerGroupSession, logger *slog.Logger,
	commitInterval time.Duration,
) *sessionTracker {
	tracker := &sessionTracker{
		session:        session,
		logger:         logger,
		commitInterval: commitInterval,
		partitions:     make(map[partitionKey]*partitionTracker),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}

	go tracker.commitLoop()

	return tracker
}

// partition returns the tracker of the given partition, creating it if
// necessary.
func (st *sessionTracker) partition(topic string, partition int32) *partitionTracker {
	key := partitionKey{topic: topic, partition: partition}

	st.mu.Lock()
	defer st.mu.Unlock()

	if tracker, ok := st.partitions[key]; ok {
		return tracker
	}
	tracker := &partitionTracker{
		session:     st,
		key:         key,
		outstanding: make(map[int64]struct{}),
	}
	st.partitions[key] = tracker

	return tracker
}

// markOffset marks the next offset to be read for the partition. The call is
// ignored once the tracker has been closed by a rebalance or a shutdown, so a
// stale session can not advance the offsets of a newer one.
func (st *sessionTracker) markOffset(key partitionKey, offset int64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.closed {
		return
	}
	st.session.MarkOffset(key.topic, key.partition, offset, "")
	st.dirty = true
}

// commitLoop periodically commits the offsets marked by the tracker, so the
// progress is persisted even if the consumer group is configured with
// automatic commits disabled.
func (st *sessionTracker) commitLoop() {
	defer close(st.done)

	ticker := time.NewTicker(st.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			st.commit()
		case <-st.stop:
			return
		}
	}
}

// commit commits the marked offsets if any offset has been marked since the
// previous commit.
func (st *sessionTracker) commit() {
	st.mu.Lock()
	dirty := st.dirty
	st.dirty = false
	st.mu.Unlock()

	if !dirty {
		return
	}

	st.commitMu.Lock()
	defer st.commitMu.Unlock()
	st.session.Commit()
}

// close stops the commit loop and performs a final commit of the offsets that
// were acknowledged before the session ended. Records that were not completed
// remain uncommitted and are redelivered to the next session.
func (st *sessionTracker) close() {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true
	dirty := st.dirty
	st.mu.Unlock()

	close(st.stop)
	<-st.done

	if dirty {
		st.commitMu.Lock()
		st.session.Commit()
		st.commitMu.Unlock()
	}
}

// partitionTracker tracks the acknowledgment state of a single topic
// partition. It advances the committable offset only over the offsets that
// were delivered and completed consecutively in the current session.
type partitionTracker struct {
	session *sessionTracker
	key     partitionKey

	mu sync.Mutex
	// started reports whether an offset has been delivered in this session.
	started bool
	// next is the next offset to commit, i.e. the first offset that has not
	// been completed yet.
	next int64
	// maxDelivered is the highest offset handed down to the pipeline.
	maxDelivered int64
	// outstanding contains the delivered offsets that have not been
	// completed yet.
	outstanding map[int64]struct{}
}

// deliver registers an offset that is about to be handed down to the
// pipeline. The record must be completed before the partition offset can
// advance past it.
func (p *partitionTracker) deliver(offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		// Anchor the committable offset at the first record of the claim, so
		// the offsets consumed by the previous session are never marked again.
		p.started = true
		p.next = offset
	}
	if offset < p.next {
		// the offset has already been committed; nothing to track.
		return
	}
	p.outstanding[offset] = struct{}{}
	if offset > p.maxDelivered {
		p.maxDelivered = offset
	}
}

// complete marks the delivered offset as processed and advances the
// committable offset over the consecutive completed offsets.
func (p *partitionTracker) complete(offset int64) {
	p.mu.Lock()
	if _, ok := p.outstanding[offset]; !ok {
		// unknown or already completed offset; ignore a duplicate or a late
		// acknowledgment.
		p.mu.Unlock()
		return
	}
	delete(p.outstanding, offset)
	next, advanced := p.advance()
	p.mu.Unlock()

	if advanced {
		p.session.markOffset(p.key, next)
	}
}

// advance moves next to the first offset that has not been completed. Offsets
// that were not delivered in this session, e.g. removed by log compaction, are
// skipped, but an outstanding offset always stops the advancement, so a later
// acknowledgment can not overtake an earlier unfinished record.
// The caller must hold p.mu.
func (p *partitionTracker) advance() (int64, bool) {
	advanced := false

	// Delivery is sequential per partition, hence every offset between next
	// and maxDelivered was either delivered in this session or does not exist.
	for p.next <= p.maxDelivered {
		if _, ok := p.outstanding[p.next]; ok {
			break
		}
		p.next++
		advanced = true
	}

	return p.next, advanced
}
