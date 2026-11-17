package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// SaramaSource represents an Apache Kafka source connector.
type SaramaSource struct {
	consumer sarama.ConsumerGroup
	handler  sarama.ConsumerGroupHandler
	topics   []string
	out      chan any

	logger *slog.Logger
}

var _ streams.Source = (*SaramaSource)(nil)

// NewSaramaSource returns a new [SaramaSource] connector.
//
// By default, messages are marked as consumed as soon as they are handed
// downstream, and the messages are emitted as [sarama.ConsumerMessage] values.
// Use the [WithAckTracking] option to require the pipeline to confirm that a
// message has been processed before the source advances the partition offset.
func NewSaramaSource(ctx context.Context, consumerGroup sarama.ConsumerGroup,
	topics []string, logger *slog.Logger, opts ...SourceOption,
) *SaramaSource {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "kafka.sarama"),
		slog.String("type", "source")))

	options := applySourceOptions(opts...)

	out := make(chan any)
	handler := &groupHandler{
		ready:  make(chan struct{}),
		out:    out,
		logger: logger,
		opts:   options,
	}

	saramaSource := &SaramaSource{
		consumer: consumerGroup,
		handler:  handler,
		topics:   topics,
		out:      out,
		logger:   logger,
	}

	// asynchronously consume messages and send them downstream
	go saramaSource.process(ctx)

	return saramaSource
}

func (ks *SaramaSource) process(ctx context.Context) {
loop:
	for {
		handler := ks.handler.(*groupHandler)
		// Consume is called inside an infinite loop, so that when a
		// server-side rebalance happens, the consumer session will be
		// recreated to get the new claims.
		if err := ks.consumer.Consume(ctx, ks.topics, handler); err != nil {
			ks.logger.Error("Error in consumer.Consume", slog.Any("error", err))
		}
		handler.ready = make(chan struct{})

		select {
		case <-ctx.Done():
			break loop
		default:
		}
	}

	ks.logger.Info("Closing connector")
	close(ks.out)

	if err := ks.consumer.Close(); err != nil {
		ks.logger.Warn("Error in consumer.Close", slog.Any("error", err))
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (ks *SaramaSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ks, operator)
	return operator
}

// Out returns the output channel of the KafkaSource connector.
func (ks *SaramaSource) Out() <-chan any {
	return ks.out
}

// groupHandler implements the [sarama.ConsumerGroupHandler] interface.
// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for the consumer group session life-cycle and
// allows for triggering logic before or after the consume loop(s).
type groupHandler struct {
	ready  chan struct{}
	out    chan any
	logger *slog.Logger
	opts   sourceOptions

	// tracker is the acknowledgment tracker of the current consumer session.
	// Sarama runs the sessions of a handler sequentially, so a single tracker
	// is sufficient. A tracker is closed by the Cleanup hook and ignores any
	// acknowledgment that arrives afterwards, which prevents a late result
	// from advancing the offsets of a newer session.
	trackerMu sync.Mutex
	tracker   *sessionTracker
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (handler *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	handler.trackerMu.Lock()
	previous := handler.tracker
	handler.tracker = nil
	handler.trackerMu.Unlock()
	if previous != nil {
		// A session must be cleaned up before the next one starts, but make
		// sure a tracker never outlives its session.
		previous.close()
	}

	if handler.opts.ackTracking {
		tracker := newSessionTracker(session, handler.logger, handler.opts.commitInterval)
		handler.trackerMu.Lock()
		handler.tracker = tracker
		handler.trackerMu.Unlock()
	}

	// mark the consumer as ready
	close(handler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (handler *groupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	handler.trackerMu.Lock()
	tracker := handler.tracker
	handler.tracker = nil
	handler.trackerMu.Unlock()

	if tracker != nil {
		// Commit the consecutively completed offsets of the session that is
		// ending. Records that were not completed stay uncommitted and are
		// redelivered to the next session.
		tracker.close()
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (handler *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	if handler.opts.ackTracking {
		return handler.consumeClaimWithAck(session, claim)
	}
	return handler.consumeClaim(session, claim)
}

// consumeClaim forwards the claimed messages downstream and immediately marks
// them as consumed. It is used when acknowledgment tracking is disabled.
func (handler *groupHandler) consumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				// The messages channel is closed when the partition consumer
				// is stopped, which does not necessarily end the session.
				// Returning lets the session end and the claim be renewed.
				return nil
			}
			if message == nil {
				continue
			}
			handler.logMessage(message)
			session.MarkMessage(message, "") // mark the message as consumed
			select {
			case handler.out <- message:
			case <-session.Context().Done():
				return session.Context().Err()
			}

		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// consumeClaimWithAck forwards the claimed records downstream wrapped into
// [Record] values. The offsets are marked only when the records are
// acknowledged by the pipeline, and only over the consecutively completed
// records of each partition.
func (handler *groupHandler) consumeClaimWithAck(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	tracker := handler.sessionTracker()
	if tracker == nil {
		return fmt.Errorf("no acknowledgment tracker for the consumer session")
	}
	partition := tracker.partition(claim.Topic(), claim.Partition())

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if message == nil {
				continue
			}
			handler.logMessage(message)

			record := &Record{
				ConsumerMessage: message,
				partition:       partition,
				offset:          message.Offset,
			}
			// Register the record before it is handed downstream, so a
			// concurrent acknowledgment can not be lost. Note that the offset
			// is not marked as consumed here: it becomes committable only when
			// the record is completed.
			partition.deliver(message.Offset)
			select {
			case handler.out <- record:
			case <-session.Context().Done():
				return session.Context().Err()
			}

		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// sessionTracker returns the acknowledgment tracker of the current session.
func (handler *groupHandler) sessionTracker() *sessionTracker {
	handler.trackerMu.Lock()
	defer handler.trackerMu.Unlock()
	return handler.tracker
}

func (handler *groupHandler) logMessage(message *sarama.ConsumerMessage) {
	handler.logger.Debug("Message claimed",
		slog.String("value", string(message.Value)),
		slog.Any("timestamp", message.Timestamp),
		slog.String("topic", message.Topic))
}

// SaramaSink represents an Apache Kafka sink connector.
type SaramaSink struct {
	producer sarama.SyncProducer
	topic    string
	in       chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*SaramaSink)(nil)

// NewSaramaSink returns a new [SaramaSink] connector.
func NewSaramaSink(syncProducer sarama.SyncProducer, topic string,
	logger *slog.Logger) *SaramaSink {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "kafka.sarama"),
		slog.String("type", "sink")))

	saramaSink := &SaramaSink{
		producer: syncProducer,
		topic:    topic,
		in:       make(chan any),
		done:     make(chan struct{}),
		logger:   logger,
	}

	// begin processing upstream records
	go saramaSink.process()

	return saramaSink
}

func (ks *SaramaSink) process() {
	defer close(ks.done) // signal data processing completion

	for msg := range ks.in {
		ks.handle(msg)
	}

	ks.logger.Info("Closing connector")
	if err := ks.producer.Close(); err != nil {
		ks.logger.Warn("Error in producer.Close", slog.Any("error", err))
	}
}

// handle writes a single stream element to the target topic and acknowledges
// the records it carries according to the write result.
func (ks *SaramaSink) handle(element any) {
	switch message := element.(type) {
	case *Record:
		if message == nil || message.ConsumerMessage == nil {
			return
		}
		err := ks.send(message.ConsumerMessage)
		ks.confirm([]*Record{message}, err)
	case []*Record:
		for _, record := range message {
			if record == nil || record.ConsumerMessage == nil {
				continue
			}
			err := ks.send(record.ConsumerMessage)
			ks.confirm([]*Record{record}, err)
		}
	case []any:
		for _, item := range message {
			ks.handle(item)
		}
	case *sarama.ProducerMessage:
		_, _, err := ks.producer.SendMessage(message)
		if err != nil {
			ks.logger.Error("Error processing message", slog.Any("error", err))
		}
	case *sarama.ConsumerMessage:
		ks.logSendResult(ks.send(message))
	case string:
		producerMessage := &sarama.ProducerMessage{
			Topic: ks.topic,
			Value: sarama.StringEncoder(message),
		}
		ks.logSendResult(ks.sendMessage(producerMessage))
	default:
		ks.logger.Error("Unsupported message type",
			slog.String("type", fmt.Sprintf("%T", message)))
	}
}

// send produces the given consumed message to the target topic.
func (ks *SaramaSink) send(message *sarama.ConsumerMessage) error {
	producerMessage := &sarama.ProducerMessage{
		Topic: ks.topic,
		Key:   sarama.StringEncoder(message.Key),
		Value: sarama.StringEncoder(message.Value),
	}
	return ks.sendMessage(producerMessage)
}

// sendMessage produces the given message to the target topic.
func (ks *SaramaSink) sendMessage(message *sarama.ProducerMessage) error {
	_, _, err := ks.producer.SendMessage(message)
	return err
}

// logSendResult logs a write error of an element that carries no records.
func (ks *SaramaSink) logSendResult(err error) {
	if err != nil {
		ks.logger.Error("Error processing message", slog.Any("error", err))
	}
}

// confirm acknowledges the records processed by the sink. Records are
// acknowledged when the write has succeeded, and are reported as failed
// otherwise, which keeps the source from committing their offsets.
func (ks *SaramaSink) confirm(records []*Record, err error) {
	if err != nil {
		ks.logger.Error("Error processing message", slog.Any("error", err))
	}
	for _, record := range records {
		if err != nil {
			record.Fail(err)
			continue
		}
		record.Ack()
	}
}

// In returns the input channel of the SaramaSink connector.
func (ks *SaramaSink) In() chan<- any {
	return ks.in
}

// AwaitCompletion blocks until the SaramaSink connector has completed
// processing all the received data.
func (ks *SaramaSink) AwaitCompletion() {
	<-ks.done
}
