package main

import (
	"fmt"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

// This example demonstrates an as-of join that enriches orders with the
// customer level effective at the order time.
//
// The order and the customer level streams are independent and may deliver
// records out of order. The join pairs records by business key and event time,
// not by arrival order:
//   - an order takes the latest level whose effective time is not later than
//     the order time;
//   - a deletion resets the level, so later orders are written with an empty
//     level until a newer level arrives;
//   - an order is finalized only after the watermarks of both sides have
//     passed its event time;
//   - stale records are routed to the late output, conflicting dimension
//     versions to the reject output.
//
// Results are mapped to text and written to a file.
func main() {
	customer := "c-42"
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	orderIn := make(chan any)
	levelIn := make(chan any)

	join := flow.NewAsOfJoin[string, order, level]()

	// independently consume the late and rejected records
	go func() {
		for record := range join.LateOut() {
			fmt.Println("late:", record)
		}
	}()
	go func() {
		for reject := range join.RejectOut() {
			fmt.Println("reject:", reject)
		}
	}()

	toLine := flow.NewMap(func(result flow.AsOfJoinResult[string, order, level]) string {
		name := result.Dimension.name
		if !result.HasDimension {
			name = "<none>"
		}
		return fmt.Sprintf("order=%s time=%s level=%s\n",
			result.Order.id, result.OrderTime.Format(time.RFC3339), name)
	}, 1)
	sink := ext.NewFileSink("out.txt")

	go produceOrders(orderIn, join, customer, base)
	go produceLevels(levelIn, join, customer, base)

	flow.Merge(
		ext.NewChanSource(orderIn).Via(flow.NewPassThrough()),
		ext.NewChanSource(levelIn).Via(flow.NewPassThrough()),
	).
		Via(join).
		Via(toLine).
		To(sink)
}

// produceOrders emits the fact stream. It runs concurrently with the
// dimension producer, so arrival order differs from event-time order.
func produceOrders(in chan any, join *flow.AsOfJoin[string, order, level],
	customer string, base time.Time) {
	defer close(in)
	in <- join.OrderEvent(customer, base.Add(15*time.Minute), order{id: "o-1"})
	in <- join.OrderEvent(customer, base.Add(5*time.Minute), order{id: "o-2"})
	in <- join.OrderEvent(customer, base.Add(35*time.Minute), order{id: "o-3"})
	in <- join.WatermarkEvent(flow.AsOfSideOrder, base.Add(time.Hour))
	// this order is below the order watermark and becomes late
	in <- join.OrderEvent(customer, base.Add(40*time.Minute), order{id: "o-4"})
}

// produceLevels emits the dimension stream; some updates arrive after the
// orders that they affect.
func produceLevels(in chan any, join *flow.AsOfJoin[string, order, level],
	customer string, base time.Time) {
	defer close(in)
	in <- join.DimensionEvent(customer, base.Add(10*time.Minute), level{name: "gold"})
	in <- join.DimensionEvent(customer, base.Add(20*time.Minute), level{name: "silver"})
	in <- join.DimensionDeleteEvent(customer, base.Add(30*time.Minute))
	in <- join.WatermarkEvent(flow.AsOfSideDimension, base.Add(time.Hour))
}

type order struct {
	id string
}

type level struct {
	name string
}
