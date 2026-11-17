package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/kafka"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	hosts := []string{"127.0.0.1:9092"}
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// The acknowledgment chain controls the offsets manually, so the
	// automatic offset commits must be disabled.
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Producer.Return.Successes = true
	config.Version, _ = sarama.ParseKafkaVersion("3.6.2")
	groupID := "testConsumer"

	consumerGroup, err := sarama.NewConsumerGroup(hosts, groupID, config)
	if err != nil {
		log.Fatal(err)
	}

	// Emit records that must be confirmed by the pipeline. A record is
	// committed only after the sink has written it, or after a flow has
	// explicitly discarded it.
	source := ext.NewSaramaSource(ctx, consumerGroup, []string{"test"}, nil,
		ext.WithAckTracking())

	syncProducer, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		log.Fatal(err)
	}

	// The sink acknowledges the records it has successfully written.
	sink := ext.NewSaramaSink(syncProducer, "test2", nil)

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	throttler := flow.NewThrottler(1, time.Second, 50, flow.Discard)
	tumblingWindow := flow.NewTumblingWindow[*ext.Record](time.Second * 5)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(appendAsterisk, 1)

	source.
		Via(toUpperMapFlow).
		Via(throttler).
		Via(tumblingWindow).
		Via(appendAsteriskFlatMapFlow).
		To(sink)
}

var toUpper = func(record *ext.Record) *ext.Record {
	record.Value = []byte(strings.ToUpper(string(record.Value)))
	return record
}

var appendAsterisk = func(inRecords []*ext.Record) []*ext.Record {
	for _, record := range inRecords {
		record.Value = []byte(string(record.Value) + "*")
	}
	return inRecords
}
