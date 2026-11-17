package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// integrationBrokersEnv is the environment variable holding the comma-separated
// list of Kafka brokers used by the integration tests. The tests are skipped
// when the variable is not set.
const integrationBrokersEnv = "KAFKA_BOOTSTRAP_SERVERS"

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	value := os.Getenv(integrationBrokersEnv)
	if value == "" {
		t.Skipf("%s is not set", integrationBrokersEnv)
	}
	return strings.Split(value, ",")
}

// createTopic creates a topic with a single partition and waits for it to
// become available.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		t.Fatalf("failed to create cluster admin: %v", err)
	}
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("failed to create topic: %v", err)
	}
}

// newConsumerGroup creates a consumer group with automatic offset commits
// disabled, which is required for manual acknowledgment tracking.
func newConsumerGroup(t *testing.T, brokers []string, group string) sarama.ConsumerGroup {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}
	t.Cleanup(func() { _ = consumerGroup.Close() })

	return consumerGroup
}

// newSyncProducer creates a producer that reports successful writes.
func newSyncProducer(t *testing.T, brokers []string) sarama.SyncProducer {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}

	return producer
}

func TestIntegrationAckChainCommitsConsecutivelyCompleted(t *testing.T) {
	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("go-streams-ack-%d", time.Now().UnixNano())
	group := topic + "-group"
	createTopic(t, brokers, topic)

	producer := newSyncProducer(t, brokers)
	defer func() { _ = producer.Close() }()

	const count = 10
	for i := 0; i < count; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerGroup := newConsumerGroup(t, brokers, group)
	source := NewSaramaSource(ctx, consumerGroup, []string{topic}, testLogger(),
		WithAckTracking(), WithCommitInterval(100*time.Millisecond))

	// acknowledge the records from the second one onwards, while keeping the
	// first record outstanding; the offsets of the partition must not advance
	// past it, even though every later record has been processed.
	var received []*Record
	for len(received) < count {
		select {
		case element := <-source.Out():
			record := element.(*Record)
			received = append(received, record)
			if record.Offset > 0 {
				record.Ack()
			} else {
				record.Fail(errors.New("simulated processing failure"))
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for records, received %d", len(received))
		}
	}

	// give the periodic commit loop a chance to persist the progress; the
	// committed offset must remain unset, since the first record is incomplete.
	time.Sleep(500 * time.Millisecond)
	if committed := committedOffset(t, brokers, group, topic); committed >= 0 {
		t.Fatalf("expected no committed offset, got %d", committed)
	}

	// completing the first record releases the chain
	received[0].Ack()
	committed := waitCommittedOffset(t, brokers, group, topic, count)
	eqInt64(t, committed, count)
}

func TestIntegrationAckAfterRebalance(t *testing.T) {
	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("go-streams-rebalance-%d", time.Now().UnixNano())
	group := topic + "-group"
	createTopic(t, brokers, topic)

	producer := newSyncProducer(t, brokers)
	defer func() { _ = producer.Close() }()

	for i := 0; i < 3; i++ {
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("value-%d", i)),
		}); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstGroup := newConsumerGroup(t, brokers, group)
	first := NewSaramaSource(ctx, firstGroup, []string{topic}, testLogger(),
		WithAckTracking(), WithCommitInterval(100*time.Millisecond))

	var oldRecords []*Record
	for len(oldRecords) < 3 {
		select {
		case element := <-first.Out():
			oldRecords = append(oldRecords, element.(*Record))
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for records, received %d", len(oldRecords))
		}
	}

	// acknowledge the first two records and stop the first consumer, which
	// ends its session; the third record stays unacknowledged.
	oldRecords[0].Ack()
	oldRecords[1].Ack()
	waitCommittedOffset(t, brokers, group, topic, 2)
	cancel()
	_ = firstGroup.Close()

	// a new consumer session in the same group redelivers the incomplete
	// record.
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	secondGroup := newConsumerGroup(t, brokers, group)
	second := NewSaramaSource(newCtx, secondGroup, []string{topic}, testLogger(),
		WithAckTracking(), WithCommitInterval(100*time.Millisecond))

	var redelivered []*Record
	for len(redelivered) == 0 {
		select {
		case element := <-second.Out():
			redelivered = append(redelivered, element.(*Record))
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for the redelivered record")
		}
	}
	eqInt64(t, redelivered[0].Offset, 2)

	// the late acknowledgment of a record from the previous session must not
	// advance the offsets of the new session.
	oldRecords[2].Ack()
	time.Sleep(300 * time.Millisecond)
	committed := committedOffset(t, brokers, group, topic)
	eqInt64(t, committed, 2)

	// the new session can complete the record and commit the progress
	redelivered[0].Ack()
	eqInt64(t, waitCommittedOffset(t, brokers, group, topic, 3), 3)
}

func TestIntegrationSinkAcknowledgesWrites(t *testing.T) {
	brokers := integrationBrokers(t)
	sourceTopic := fmt.Sprintf("go-streams-sink-source-%d", time.Now().UnixNano())
	targetTopic := sourceTopic + "-target"
	group := sourceTopic + "-group"
	createTopic(t, brokers, sourceTopic)
	createTopic(t, brokers, targetTopic)

	producer := newSyncProducer(t, brokers)
	defer func() { _ = producer.Close() }()

	const count = 5
	for i := 0; i < count; i++ {
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: sourceTopic,
			Value: sarama.StringEncoder(fmt.Sprintf("value-%d", i)),
		}); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerGroup := newConsumerGroup(t, brokers, group)
	source := NewSaramaSource(ctx, consumerGroup, []string{sourceTopic}, testLogger(),
		WithAckTracking(), WithCommitInterval(100*time.Millisecond))

	sinkProducer := newSyncProducer(t, brokers)
	sink := NewSaramaSink(sinkProducer, targetTopic, testLogger())

	go func() {
		for element := range source.Out() {
			sink.In() <- element
		}
		close(sink.In())
	}()

	eqInt64(t, waitCommittedOffset(t, brokers, group, sourceTopic, count), count)
}

// committedOffset returns the committed offset of the single partition of the
// topic for the given consumer group, or -1 if no offset has been committed.
func committedOffset(t *testing.T, brokers []string, group, topic string) int64 {
	t.Helper()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		t.Fatalf("failed to create offset manager: %v", err)
	}
	defer func() { _ = offsetManager.Close() }()

	partitionOffsetManager, err := offsetManager.ManagePartition(topic, 0)
	if err != nil {
		t.Fatalf("failed to manage partition: %v", err)
	}
	defer partitionOffsetManager.Close()

	offset, _ := partitionOffsetManager.NextOffset()
	return offset
}

// waitCommittedOffset polls the committed offset until it reaches the expected
// value or the timeout expires.
func waitCommittedOffset(t *testing.T, brokers []string, group, topic string,
	expected int64,
) int64 {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var offset int64
	for time.Now().Before(deadline) {
		offset = committedOffset(t, brokers, group, topic)
		if offset >= expected {
			return offset
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for the committed offset %d of %s, got %d",
		expected, topic, offset)
	return offset
}
