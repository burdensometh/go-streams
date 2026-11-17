package flow_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

type asOfOrder struct {
	id     string
	amount int
}

type asOfLevel struct {
	name     string
	discount int
}

const (
	testAsOfKey   = "c1"
	levelGold     = "gold"
	levelPlatinum = "platinum"
)

type asOfJoin = flow.AsOfJoin[string, asOfOrder, asOfLevel]

type asOfResult = flow.AsOfJoinResult[string, asOfOrder, asOfLevel]

type asOfLate = flow.AsOfLate[string, asOfOrder, asOfLevel]

func newTestAsOfJoin() *asOfJoin {
	return flow.NewAsOfJoin[string, asOfOrder, asOfLevel]()
}

func asOfTime(second int) time.Time {
	return time.Unix(int64(second), 0).UTC()
}

func collectAsOf(t *testing.T, join *asOfJoin, input []any) (
	[]asOfResult, []asOfLate, []flow.AsOfJoinError,
) {
	t.Helper()

	var wg sync.WaitGroup
	wg.Add(2)

	var late []asOfLate
	var rejects []flow.AsOfJoinError

	go func() {
		defer wg.Done()
		for record := range join.LateOut() {
			late = append(late, record)
		}
	}()
	go func() {
		defer wg.Done()
		for reject := range join.RejectOut() {
			rejects = append(rejects, reject)
		}
	}()

	go func() {
		for _, event := range input {
			join.In() <- event
		}
		close(join.In())
	}()

	var results []asOfResult
	for value := range join.Out() {
		results = append(results, value.(asOfResult))
	}

	wg.Wait()
	return results, late, rejects
}

func resultSignature(result asOfResult) string {
	return result.Order.id + ":" + result.Dimension.name
}

func TestAsOfJoin_ResolvesByEventTimeNotArrivalOrder(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		// orders and dimension updates arrive interleaved and out of order
		join.OrderEvent(testAsOfKey, asOfTime(15), asOfOrder{"o15", 15}),
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.DimensionEvent(testAsOfKey, asOfTime(20), asOfLevel{levelPlatinum, 10}),
		join.OrderEvent(testAsOfKey, asOfTime(25), asOfOrder{"o25", 25}),
		join.OrderEvent(testAsOfKey, asOfTime(5), asOfOrder{"o5", 5}),
		join.DimensionDeleteEvent(testAsOfKey, asOfTime(30)),
		join.OrderEvent(testAsOfKey, asOfTime(35), asOfOrder{"o35", 35}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, late, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(late))
	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 4, len(results))

	// finalized results are ordered by event time, not by arrival order
	expectedOrders := []string{"o5", "o15", "o25", "o35"}
	expectedLevels := []string{"", levelGold, levelPlatinum, ""}
	for i, result := range results {
		assert.Equal(t, expectedOrders[i], result.Order.id)
		assert.Equal(t, expectedLevels[i], result.Dimension.name)
		assert.Equal(t, asOfTime(map[string]int{"o5": 5, "o15": 15, "o25": 25, "o35": 35}[result.Order.id]),
			result.OrderTime)
	}

	// the first order precedes every dimension version
	assert.Equal(t, false, results[0].HasDimension)
	assert.Equal(t, false, results[0].Deleted)
	// the last order follows the tombstone: an empty level, explicitly deleted
	assert.Equal(t, false, results[3].HasDimension)
	assert.Equal(t, true, results[3].Deleted)
	assert.Equal(t, asOfTime(30), results[3].DimensionTime)

	stats := join.Stats()
	assert.Equal(t, uint64(4), stats.AcceptedOrders)
	assert.Equal(t, uint64(3), stats.AcceptedDimensions)
	assert.Equal(t, uint64(4), stats.Finalized)
	assert.Equal(t, uint64(2), stats.Matched)
	assert.Equal(t, uint64(1), stats.Unmatched)
	assert.Equal(t, uint64(1), stats.Deleted)
}

func TestAsOfJoin_ChronologicalMatchedBoundary(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		// a dimension exactly at the order time is not later than the order
		join.OrderEvent(testAsOfKey, asOfTime(10), asOfOrder{"equal", 10}),
		join.OrderEvent(testAsOfKey, asOfTime(11), asOfOrder{"after", 11}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, _, _ := collectAsOf(t, join, input)
	assert.Equal(t, 2, len(results))

	signatures := []string{resultSignature(results[0]), resultSignature(results[1])}
	assert.Equal(t, []string{"equal:gold", "after:gold"}, signatures)
	assert.Equal(t, asOfTime(10), results[0].DimensionTime)
}

func TestAsOfJoin_WaitsForBothWatermarks(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.OrderEvent(testAsOfKey, asOfTime(20), asOfOrder{"o20", 20}),
		// the order watermark alone must not finalize the order
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(30)),
		// this dimension arrives after the order watermark, but the dimension
		// watermark has not passed yet, so it is still accepted
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(30)),
	}

	results, late, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(late))
	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "o20", results[0].Order.id)
	assert.Equal(t, levelGold, results[0].Dimension.name)
	assert.Equal(t, true, results[0].HasDimension)
}

func TestAsOfJoin_EmitsLateRecords(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.OrderEvent(testAsOfKey, asOfTime(10), asOfOrder{"o10", 10}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(50)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(50)),
		// stale records: both event times precede their side's watermark
		join.OrderEvent(testAsOfKey, asOfTime(40), asOfOrder{"o40", 40}),
		join.DimensionEvent(testAsOfKey, asOfTime(20), asOfLevel{levelGold, 5}),
		// a record at the watermark is not late
		join.DimensionEvent(testAsOfKey, asOfTime(50), asOfLevel{levelPlatinum, 10}),
	}

	results, late, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "o10", results[0].Order.id)
	assert.Equal(t, false, results[0].HasDimension)

	assert.Equal(t, 2, len(late))
	lateSides := []flow.AsOfSide{late[0].Side, late[1].Side}
	expectedSides := []flow.AsOfSide{flow.AsOfSideOrder, flow.AsOfSideDimension}
	assert.Equal(t, expectedSides, lateSides)
	assert.Equal(t, "o40", late[0].Order.id)
	assert.Equal(t, asOfTime(40), late[0].Time)
	assert.Equal(t, asOfTime(20), late[1].Time)

	stats := join.Stats()
	assert.Equal(t, uint64(1), stats.LateOrders)
	assert.Equal(t, uint64(1), stats.LateDimensions)

	// the stale order was never joined
	for _, result := range results {
		assert.Equal(t, false, result.Order.id == "o40")
	}
}

func TestAsOfJoin_IsolatesKeys(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.DimensionEvent("a", asOfTime(10), asOfLevel{levelGold, 5}),
		join.DimensionEvent("b", asOfTime(20), asOfLevel{levelPlatinum, 10}),
		// each key resolves against its own dimension history
		join.OrderEvent("a", asOfTime(15), asOfOrder{"a15", 15}),
		join.OrderEvent("b", asOfTime(15), asOfOrder{"b15", 15}),
		join.OrderEvent("c", asOfTime(15), asOfOrder{"c15", 15}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, _, _ := collectAsOf(t, join, input)
	assert.Equal(t, 3, len(results))

	levels := make(map[string]string)
	for _, result := range results {
		levels[result.Key] = result.Dimension.name
	}
	assert.Equal(t, map[string]string{"a": levelGold, "b": "", "c": ""}, levels)
}

func TestAsOfJoin_AcceptsPointerInput(t *testing.T) {
	join := newTestAsOfJoin()
	orderInput := join.OrderEvent(testAsOfKey, asOfTime(10), asOfOrder{"o10", 10})
	dimensionInput := join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5})
	watermark := join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100))
	dimensionWatermark := join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100))

	results, _, rejects := collectAsOf(t, join, []any{
		&orderInput, &dimensionInput, &watermark, &dimensionWatermark,
	})

	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "o10:gold", resultSignature(results[0]))
}

func TestAsOfJoin_OutOfOrderDimensionVersions(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		// versions arrive in reverse event-time order
		join.DimensionEvent(testAsOfKey, asOfTime(30), asOfLevel{"diamond", 20}),
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.DimensionEvent(testAsOfKey, asOfTime(20), asOfLevel{levelPlatinum, 10}),
		join.OrderEvent(testAsOfKey, asOfTime(25), asOfOrder{"o25", 25}),
		join.OrderEvent(testAsOfKey, asOfTime(35), asOfOrder{"o35", 35}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, _, rejects := collectAsOf(t, join, input)
	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 2, len(results))
	assert.Equal(t, []string{"o25:platinum", "o35:diamond"},
		[]string{resultSignature(results[0]), resultSignature(results[1])})
}

func TestAsOfJoin_TombstoneIdempotentAndResurrection(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.DimensionDeleteEvent(testAsOfKey, asOfTime(20)),
		// an identical tombstone is idempotent
		join.DimensionDeleteEvent(testAsOfKey, asOfTime(20)),
		join.OrderEvent(testAsOfKey, asOfTime(25), asOfOrder{"o25", 25}),
		// a newer version restores a level after the deletion
		join.DimensionEvent(testAsOfKey, asOfTime(30), asOfLevel{levelPlatinum, 10}),
		join.OrderEvent(testAsOfKey, asOfTime(35), asOfOrder{"o35", 35}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, _, rejects := collectAsOf(t, join, input)
	assert.Equal(t, 0, len(rejects))
	assert.Equal(t, 2, len(results))

	assert.Equal(t, false, results[0].HasDimension)
	assert.Equal(t, true, results[0].Deleted)
	assert.Equal(t, "o35:platinum", resultSignature(results[1]))
	assert.Equal(t, true, results[1].HasDimension)

	stats := join.Stats()
	assert.Equal(t, uint64(1), stats.DuplicateDimensions)
	assert.Equal(t, uint64(3), stats.AcceptedDimensions)
}

func TestAsOfJoin_RejectsDimensionConflict(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{"silver", 2}),
		// an exact duplicate is idempotent and not a conflict
		join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		join.OrderEvent(testAsOfKey, asOfTime(10), asOfOrder{"o10", 10}),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100)),
	}

	results, late, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(late))
	assert.Equal(t, 1, len(results))
	assert.Equal(t, levelGold, results[0].Dimension.name)

	assert.Equal(t, 1, len(rejects))
	assert.Equal(t, flow.AsOfJoinErrorDimensionConflict, rejects[0].Kind)
	assert.Equal(t, testAsOfKey, rejects[0].Key)
	assert.Equal(t, asOfTime(10), rejects[0].Time)
	existing, ok := rejects[0].Existing.(asOfLevel)
	assert.Equal(t, true, ok)
	assert.Equal(t, levelGold, existing.name)

	stats := join.Stats()
	assert.Equal(t, uint64(1), stats.Rejected)
	assert.Equal(t, uint64(1), stats.AcceptedDimensions)
	assert.Equal(t, uint64(1), stats.DuplicateDimensions)
}

func TestAsOfJoin_RejectsWatermarkRegression(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(50)),
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(40)),
		// repeating the current watermark is allowed
		join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(50)),
		join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(50)),
	}

	results, late, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(results))
	assert.Equal(t, 0, len(late))
	assert.Equal(t, 1, len(rejects))
	assert.Equal(t, flow.AsOfJoinErrorWatermarkRegression, rejects[0].Kind)
	assert.Equal(t, flow.AsOfSideOrder, rejects[0].Side)
	assert.Equal(t, asOfTime(40), rejects[0].Time)
}

func TestAsOfJoin_CompletesRemainingOrdersOnEnd(t *testing.T) {
	t.Run("end events", func(t *testing.T) {
		join := newTestAsOfJoin()
		input := []any{
			join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
			join.OrderEvent(testAsOfKey, asOfTime(5), asOfOrder{"o5", 5}),
			join.OrderEvent(testAsOfKey, asOfTime(20), asOfOrder{"o20", 20}),
			join.EndEvent(flow.AsOfSideOrder),
			join.EndEvent(flow.AsOfSideDimension),
		}

		results, _, rejects := collectAsOf(t, join, input)
		assert.Equal(t, 0, len(rejects))
		assert.Equal(t, 2, len(results))
		assert.Equal(t, "o5:", resultSignature(results[0]))
		assert.Equal(t, "o20:gold", resultSignature(results[1]))
	})

	t.Run("input channel closed", func(t *testing.T) {
		join := newTestAsOfJoin()
		input := []any{
			join.OrderEvent(testAsOfKey, asOfTime(20), asOfOrder{"o20", 20}),
			join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5}),
		}

		results, _, rejects := collectAsOf(t, join, input)
		assert.Equal(t, 0, len(rejects))
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "o20:gold", resultSignature(results[0]))
	})
}

func TestAsOfJoin_RejectsInputAfterEnd(t *testing.T) {
	join := newTestAsOfJoin()
	input := []any{
		join.EndEvent(flow.AsOfSideOrder),
		join.OrderEvent(testAsOfKey, asOfTime(10), asOfOrder{"o10", 10}),
		join.EndEvent(flow.AsOfSideDimension),
	}

	results, _, rejects := collectAsOf(t, join, input)

	assert.Equal(t, 0, len(results))
	assert.Equal(t, 1, len(rejects))
	assert.Equal(t, flow.AsOfJoinErrorClosedInput, rejects[0].Kind)
	assert.Equal(t, flow.AsOfSideOrder, rejects[0].Side)
}

func TestAsOfJoin_RejectsInvalidInput(t *testing.T) {
	join := newTestAsOfJoin()
	results, _, rejects := collectAsOf(t, join, []any{"not-an-event"})

	assert.Equal(t, 0, len(results))
	assert.Equal(t, 1, len(rejects))
	assert.Equal(t, flow.AsOfJoinErrorInvalidInput, rejects[0].Kind)
	assert.Equal(t, "not-an-event", rejects[0].Input)
}

func TestAsOfJoin_PipelineToMapAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asserted.txt")

	orderIn := make(chan any)
	dimensionIn := make(chan any)

	orderSide := ext.NewChanSource(orderIn).Via(flow.NewPassThrough())
	dimensionSide := ext.NewChanSource(dimensionIn).Via(flow.NewPassThrough())
	join := flow.NewAsOfJoin[string, asOfOrder, asOfLevel]()

	var late []asOfLate
	var lateWg sync.WaitGroup
	lateWg.Add(1)
	go func() {
		defer lateWg.Done()
		for record := range join.LateOut() {
			late = append(late, record)
		}
	}()

	toLine := flow.NewMap(func(result asOfResult) string {
		return result.Key + "|" + result.Order.id + "|" + result.Dimension.name + "\n"
	}, 1)
	sink := ext.NewFileSink(path)

	go func() {
		orderIn <- join.OrderEvent(testAsOfKey, asOfTime(15), asOfOrder{"o15", 15})
		orderIn <- join.OrderEvent(testAsOfKey, asOfTime(5), asOfOrder{"o5", 5})
		orderIn <- join.OrderEvent(testAsOfKey, asOfTime(35), asOfOrder{"o35", 35})
		orderIn <- join.WatermarkEvent(flow.AsOfSideOrder, asOfTime(100))
		// a stale order must reach the late output, not the file
		orderIn <- join.OrderEvent(testAsOfKey, asOfTime(40), asOfOrder{"o40", 40})
		close(orderIn)
	}()
	go func() {
		dimensionIn <- join.DimensionEvent(testAsOfKey, asOfTime(10), asOfLevel{levelGold, 5})
		dimensionIn <- join.DimensionDeleteEvent(testAsOfKey, asOfTime(30))
		dimensionIn <- join.WatermarkEvent(flow.AsOfSideDimension, asOfTime(100))
		close(dimensionIn)
	}()

	flow.Merge(orderSide, dimensionSide).
		Via(join).
		Via(toLine).
		To(sink)

	lateWg.Wait()

	data, err := os.ReadFile(path)
	assert.Equal(t, nil, err)
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	assert.Equal(t, []string{
		"c1|o5|",
		"c1|o15|gold",
		"c1|o35|",
	}, lines)

	assert.Equal(t, 1, len(late))
	assert.Equal(t, "o40", late[0].Order.id)

	for value := range join.RejectOut() {
		t.Fatalf("unexpected reject: %v", value)
	}
}

func TestAsOfSide_String(t *testing.T) {
	assert.Equal(t, "order", flow.AsOfSideOrder.String())
	assert.Equal(t, "dimension", flow.AsOfSideDimension.String())
	assert.Equal(t, "AsOfSide(7)", flow.AsOfSide(7).String())
}

func TestAsOfJoinError_Error(t *testing.T) {
	reject := flow.AsOfJoinError{
		Kind: flow.AsOfJoinErrorDimensionConflict,
		Side: flow.AsOfSideDimension,
		Key:  testAsOfKey,
		Time: asOfTime(10),
	}
	assert.Equal(t, "flow: dimension conflict (side=dimension key=c1 time=1970-01-01T00:00:10Z)",
		reject.Error())
	assert.Equal(t, false, reject.Input != nil)
}
