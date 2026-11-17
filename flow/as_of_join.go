package flow

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// AsOfSide identifies one of the two inputs of an AsOfJoin.
type AsOfSide uint8

const (
	// AsOfSideOrder identifies the fact stream, for example orders.
	AsOfSideOrder AsOfSide = iota
	// AsOfSideDimension identifies the versioned reference stream, for example
	// customer levels.
	AsOfSideDimension
)

// String implements the fmt.Stringer interface.
func (s AsOfSide) String() string {
	switch s {
	case AsOfSideOrder:
		return "order"
	case AsOfSideDimension:
		return "dimension"
	default:
		return fmt.Sprintf("AsOfSide(%d)", uint8(s))
	}
}

// AsOfKind distinguishes data records from event-time progress signals.
type AsOfKind uint8

const (
	// AsOfKindData marks a data record; this is the zero value.
	AsOfKindData AsOfKind = iota
	// AsOfKindWatermark marks the event-time progress of a side. No record
	// with an event time before the watermark is expected on that side.
	AsOfKindWatermark
	// AsOfKindEnd marks the end of a side's input.
	AsOfKindEnd
)

// AsOfJoinInput is one input event of an AsOfJoin. Only the payload selected
// by Side is meaningful for data records, while control events use Side,
// Kind and (for watermarks) Time.
//
// Input can be constructed directly or with the helper methods of AsOfJoin,
// for example OrderEvent, DimensionEvent, DimensionDeleteEvent,
// WatermarkEvent and EndEvent.
type AsOfJoinInput[K comparable, O, D any] struct {
	Side      AsOfSide
	Kind      AsOfKind
	Key       K
	Time      time.Time
	Order     O
	Dimension D
	// Deleted marks a dimension tombstone: from Time onwards, and until a
	// newer dimension version arrives, orders have no effective dimension.
	Deleted bool
}

// AsOfJoinResult is the finalized enrichment of one order with the latest
// dimension whose effective time is not later than the order time.
//
// HasDimension is false when no dimension was effective at the order time,
// either because no version existed yet or because the latest version was a
// deletion tombstone. Deleted distinguishes those two cases; both leave the
// effective level empty.
type AsOfJoinResult[K comparable, O, D any] struct {
	Key           K
	Order         O
	OrderTime     time.Time
	Dimension     D
	DimensionTime time.Time
	HasDimension  bool
	Deleted       bool
}

// String implements the fmt.Stringer interface.
func (r AsOfJoinResult[K, O, D]) String() string {
	level := fmt.Sprintf("%v", r.Dimension)
	if !r.HasDimension {
		level = "<empty>"
	}
	return fmt.Sprintf("key=%v order_time=%s level=%s dimension_time=%s deleted=%t",
		r.Key, r.OrderTime.Format(time.RFC3339Nano), level,
		r.DimensionTime.Format(time.RFC3339Nano), r.Deleted)
}

// AsOfLate is a data record that arrived after its own side's watermark had
// already passed its event time. Such records are not joined, even if the
// corresponding dimension history is still available.
type AsOfLate[K comparable, O, D any] struct {
	Side      AsOfSide
	Key       K
	Time      time.Time
	Order     O
	Dimension D
	Deleted   bool
}

// String implements the fmt.Stringer interface.
func (l AsOfLate[K, O, D]) String() string {
	return fmt.Sprintf("late %s key=%v time=%s", l.Side, l.Key,
		l.Time.Format(time.RFC3339Nano))
}

// AsOfJoinErrorKind identifies why an AsOfJoin input was rejected.
type AsOfJoinErrorKind uint8

const (
	// AsOfJoinErrorInvalidInput indicates a malformed or mistyped input event.
	AsOfJoinErrorInvalidInput AsOfJoinErrorKind = iota
	// AsOfJoinErrorDimensionConflict indicates two dimension records with the
	// same business key and effective time but different payloads.
	AsOfJoinErrorDimensionConflict
	// AsOfJoinErrorWatermarkRegression indicates a watermark older than the
	// current watermark of the same side.
	AsOfJoinErrorWatermarkRegression
	// AsOfJoinErrorClosedInput indicates a record received after its side ended.
	AsOfJoinErrorClosedInput
)

// String implements the fmt.Stringer interface.
func (k AsOfJoinErrorKind) String() string {
	switch k {
	case AsOfJoinErrorInvalidInput:
		return "invalid input"
	case AsOfJoinErrorDimensionConflict:
		return "dimension conflict"
	case AsOfJoinErrorWatermarkRegression:
		return "watermark regression"
	case AsOfJoinErrorClosedInput:
		return "closed input"
	default:
		return fmt.Sprintf("AsOfJoinErrorKind(%d)", uint8(k))
	}
}

// AsOfJoinError describes a rejected input. Input keeps the original dynamic
// value so the caller can route or repair it. Existing is populated for
// dimension conflicts and holds the previously accepted dimension payload.
type AsOfJoinError struct {
	Kind     AsOfJoinErrorKind
	Side     AsOfSide
	Key      any
	Time     time.Time
	Input    any
	Existing any
}

// Error implements the error interface.
func (r AsOfJoinError) Error() string {
	return fmt.Sprintf("flow: %s (side=%s key=%v time=%s)", r.Kind, r.Side, r.Key,
		r.Time.Format(time.RFC3339Nano))
}

// String implements the fmt.Stringer interface.
func (r AsOfJoinError) String() string {
	return r.Error()
}

// AsOfJoinStats is a snapshot of the join counters. PendingOrders is the
// number of orders still waiting for both watermarks to pass their time.
type AsOfJoinStats struct {
	AcceptedOrders      uint64
	AcceptedDimensions  uint64
	LateOrders          uint64
	LateDimensions      uint64
	Finalized           uint64
	Matched             uint64
	Unmatched           uint64
	Deleted             uint64
	DuplicateDimensions uint64
	Rejected            uint64
	PendingOrders       int
}

type asOfProgress struct {
	watermark    time.Time
	hasWatermark bool
	ended        bool
}

type asOfPendingOrder[K comparable, O any] struct {
	key   K
	time  time.Time
	order O
	seq   uint64
}

type asOfDimensionVersion[D any] struct {
	time    time.Time
	value   D
	deleted bool
}

// AsOfJoin enriches records from one stream with the latest version of a
// keyed, versioned reference stream effective at the record's event time.
//
// An order at event time t is joined with the dimension version having the
// greatest effective time less than or equal to t. A dimension tombstone
// (Deleted) resets the effective dimension, so later orders have an empty
// level until a newer version arrives. Version equal to the order time is
// included.
//
// An order is finalized only after the watermarks of both sides have passed
// its event time: the dimension watermark guarantees that no earlier version
// can still arrive, and the order watermark guarantees that the order itself
// is not stale. A finalization is also forced when the input ends.
//
// Records that arrive after their own side's watermark passed are emitted on
// the late output and never affect join results. Dimension records with the
// same business key and effective time but different payloads are rejected
// explicitly; identical duplicates are ignored.
//
// Input is a single stream of AsOfJoinInput events, so the two sources may be
// combined with Merge. Pairing is performed by business key and event time,
// never by arrival order.
type AsOfJoin[K comparable, O, D any] struct {
	in     chan any
	out    chan any
	late   *asOfQueue[AsOfLate[K, O, D]]
	reject *asOfQueue[AsOfJoinError]

	mu         sync.Mutex
	progress   [2]asOfProgress
	pending    []asOfPendingOrder[K, O]
	dimensions map[K][]asOfDimensionVersion[D]
	seq        uint64
	stats      AsOfJoinStats
}

// Verify AsOfJoin satisfies the Flow interface.
var _ streams.Flow = (*AsOfJoin[string, any, any])(nil)

// NewAsOfJoin returns a new AsOfJoin operator.
// K is the business key type, O is the fact stream element type, and D is the
// dimension version type.
func NewAsOfJoin[K comparable, O, D any]() *AsOfJoin[K, O, D] {
	join := &AsOfJoin[K, O, D]{
		in:         make(chan any),
		out:        make(chan any),
		late:       newAsOfQueue[AsOfLate[K, O, D]](),
		reject:     newAsOfQueue[AsOfJoinError](),
		dimensions: make(map[K][]asOfDimensionVersion[D]),
	}

	// start processing input events
	go join.run()

	return join
}

// OrderEvent returns a data input for the order side.
func (j *AsOfJoin[K, O, D]) OrderEvent(key K, eventTime time.Time, order O) AsOfJoinInput[K, O, D] {
	return AsOfJoinInput[K, O, D]{
		Side:  AsOfSideOrder,
		Kind:  AsOfKindData,
		Key:   key,
		Time:  eventTime,
		Order: order,
	}
}

// DimensionEvent returns a data input for the dimension side.
func (j *AsOfJoin[K, O, D]) DimensionEvent(key K, eventTime time.Time,
	dimension D) AsOfJoinInput[K, O, D] {
	return AsOfJoinInput[K, O, D]{
		Side:      AsOfSideDimension,
		Kind:      AsOfKindData,
		Key:       key,
		Time:      eventTime,
		Dimension: dimension,
	}
}

// DimensionDeleteEvent returns a tombstone input for the dimension side.
func (j *AsOfJoin[K, O, D]) DimensionDeleteEvent(key K,
	eventTime time.Time) AsOfJoinInput[K, O, D] {
	return AsOfJoinInput[K, O, D]{
		Side:    AsOfSideDimension,
		Kind:    AsOfKindData,
		Key:     key,
		Time:    eventTime,
		Deleted: true,
	}
}

// WatermarkEvent returns a watermark input for the specified side.
func (j *AsOfJoin[K, O, D]) WatermarkEvent(side AsOfSide,
	eventTime time.Time) AsOfJoinInput[K, O, D] {
	return AsOfJoinInput[K, O, D]{
		Side: side,
		Kind: AsOfKindWatermark,
		Time: eventTime,
	}
}

// EndEvent returns an end-of-input input for the specified side.
func (j *AsOfJoin[K, O, D]) EndEvent(side AsOfSide) AsOfJoinInput[K, O, D] {
	return AsOfJoinInput[K, O, D]{
		Side: side,
		Kind: AsOfKindEnd,
	}
}

// In returns the input channel of the AsOfJoin operator.
func (j *AsOfJoin[K, O, D]) In() chan<- any {
	return j.in
}

// Out returns the channel of finalized join results.
func (j *AsOfJoin[K, O, D]) Out() <-chan any {
	return j.out
}

// LateOut returns the channel of records that arrived after their side's
// watermark. It closes after all queued records have been delivered.
func (j *AsOfJoin[K, O, D]) LateOut() <-chan AsOfLate[K, O, D] {
	return j.late.out
}

// RejectOut returns the channel of rejected inputs. It closes after all
// queued records have been delivered.
func (j *AsOfJoin[K, O, D]) RejectOut() <-chan AsOfJoinError {
	return j.reject.out
}

// Via asynchronously streams results to the given Flow and returns it.
func (j *AsOfJoin[K, O, D]) Via(flow streams.Flow) streams.Flow {
	go j.transmit(flow)
	return flow
}

// To streams results to the given Sink and blocks until the Sink has
// completed processing all data.
func (j *AsOfJoin[K, O, D]) To(sink streams.Sink) {
	j.transmit(sink)
	sink.AwaitCompletion()
}

// transmit submits finalized results to the next Inlet.
func (j *AsOfJoin[K, O, D]) transmit(inlet streams.Inlet) {
	for result := range j.out {
		inlet.In() <- result
	}
	close(inlet.In())
}

// Stats returns a coherent snapshot of the join counters.
func (j *AsOfJoin[K, O, D]) Stats() AsOfJoinStats {
	j.mu.Lock()
	defer j.mu.Unlock()

	stats := j.stats
	stats.PendingOrders = len(j.pending)
	return stats
}

// asOfOutcome collects the effects of processing a single input event. The
// slices are nil when empty.
type asOfOutcome[K comparable, O, D any] struct {
	results []AsOfJoinResult[K, O, D]
	late    []AsOfLate[K, O, D]
	rejects []AsOfJoinError
}

// run processes input events and closes all outputs when the input ends.
func (j *AsOfJoin[K, O, D]) run() {
	for value := range j.in {
		outcome := j.apply(value)
		j.emit(outcome)
		for _, result := range outcome.results {
			j.out <- result
		}
	}

	j.mu.Lock()
	j.progress[AsOfSideOrder].ended = true
	j.progress[AsOfSideDimension].ended = true
	remaining := j.finalizeLocked(true)
	j.mu.Unlock()

	for _, result := range remaining {
		j.out <- result
	}
	j.late.close()
	j.reject.close()
	close(j.out)
}

func (j *AsOfJoin[K, O, D]) emit(outcome asOfOutcome[K, O, D]) {
	for _, record := range outcome.late {
		j.late.push(record)
	}
	for _, reject := range outcome.rejects {
		j.reject.push(reject)
	}
}

// apply validates and processes a single input event. The caller must emit
// the returned records after releasing the lock.
func (j *AsOfJoin[K, O, D]) apply(value any) asOfOutcome[K, O, D] {
	j.mu.Lock()
	defer j.mu.Unlock()

	input, ok := asOfInput[K, O, D](value)
	if !ok || input.Side > AsOfSideDimension || input.Kind > AsOfKindEnd {
		return asOfOutcome[K, O, D]{rejects: []AsOfJoinError{
			j.rejectLocked(AsOfJoinErrorInvalidInput, AsOfSideOrder, nil,
				time.Time{}, value, nil)},
		}
	}

	progress := &j.progress[input.Side]
	if progress.ended {
		return asOfOutcome[K, O, D]{rejects: []AsOfJoinError{
			j.rejectLocked(AsOfJoinErrorClosedInput, input.Side, input.Key,
				input.Time, value, nil)},
		}
	}

	switch input.Kind {
	case AsOfKindWatermark:
		return j.applyWatermarkLocked(input, value)
	case AsOfKindEnd:
		progress.ended = true
		return asOfOutcome[K, O, D]{results: j.finalizeLocked(false)}
	default:
		return j.applyDataLocked(input, value)
	}
}

func asOfInput[K comparable, O, D any](value any) (AsOfJoinInput[K, O, D], bool) {
	switch typed := value.(type) {
	case AsOfJoinInput[K, O, D]:
		return typed, true
	case *AsOfJoinInput[K, O, D]:
		if typed != nil {
			return *typed, true
		}
	}
	return AsOfJoinInput[K, O, D]{}, false
}

func (j *AsOfJoin[K, O, D]) applyWatermarkLocked(
	input AsOfJoinInput[K, O, D], value any,
) asOfOutcome[K, O, D] {
	progress := &j.progress[input.Side]
	if progress.hasWatermark && input.Time.Before(progress.watermark) {
		return asOfOutcome[K, O, D]{rejects: []AsOfJoinError{
			j.rejectLocked(AsOfJoinErrorWatermarkRegression, input.Side,
				input.Key, input.Time, value, nil)},
		}
	}
	progress.watermark = input.Time
	progress.hasWatermark = true
	return asOfOutcome[K, O, D]{results: j.finalizeLocked(false)}
}

func (j *AsOfJoin[K, O, D]) applyDataLocked(
	input AsOfJoinInput[K, O, D], value any,
) asOfOutcome[K, O, D] {
	if j.lateLocked(input) {
		return asOfOutcome[K, O, D]{late: []AsOfLate[K, O, D]{j.asLate(input)}}
	}
	if input.Side == AsOfSideOrder {
		j.stats.AcceptedOrders++
		j.seq++
		j.pending = append(j.pending, asOfPendingOrder[K, O]{
			key:   input.Key,
			time:  input.Time,
			order: input.Order,
			seq:   j.seq,
		})
		return asOfOutcome[K, O, D]{}
	}
	if reject, ok := j.upsertDimensionLocked(input, value); ok {
		return asOfOutcome[K, O, D]{rejects: []AsOfJoinError{reject}}
	}
	return asOfOutcome[K, O, D]{}
}

// lateLocked reports whether the input precedes its own side's watermark and
// updates the corresponding counter.
func (j *AsOfJoin[K, O, D]) lateLocked(input AsOfJoinInput[K, O, D]) bool {
	progress := j.progress[input.Side]
	if !progress.hasWatermark || !input.Time.Before(progress.watermark) {
		return false
	}
	if input.Side == AsOfSideOrder {
		j.stats.LateOrders++
	} else {
		j.stats.LateDimensions++
	}
	return true
}

func (j *AsOfJoin[K, O, D]) asLate(input AsOfJoinInput[K, O, D]) AsOfLate[K, O, D] {
	return AsOfLate[K, O, D]{
		Side:      input.Side,
		Key:       input.Key,
		Time:      input.Time,
		Order:     input.Order,
		Dimension: input.Dimension,
		Deleted:   input.Deleted,
	}
}

// upsertDimensionLocked inserts a dimension version, ignoring an identical
// duplicate and rejecting a conflicting version with the same effective time.
// It returns a reject and true when the input must not be applied.
func (j *AsOfJoin[K, O, D]) upsertDimensionLocked(
	input AsOfJoinInput[K, O, D], value any,
) (AsOfJoinError, bool) {
	versions := j.dimensions[input.Key]
	position := sort.Search(len(versions), func(i int) bool {
		return !versions[i].time.Before(input.Time)
	})
	if position < len(versions) && versions[position].time.Equal(input.Time) {
		existing := versions[position]
		if existing.deleted == input.Deleted && reflect.DeepEqual(existing.value, input.Dimension) {
			j.stats.DuplicateDimensions++
			return AsOfJoinError{}, false
		}
		reject := j.rejectLocked(AsOfJoinErrorDimensionConflict, input.Side,
			input.Key, input.Time, value, existing.value)
		return reject, true
	}

	versions = append(versions, asOfDimensionVersion[D]{})
	copy(versions[position+1:], versions[position:])
	versions[position] = asOfDimensionVersion[D]{
		time:    input.Time,
		value:   input.Dimension,
		deleted: input.Deleted,
	}
	j.dimensions[input.Key] = versions
	j.stats.AcceptedDimensions++
	return AsOfJoinError{}, false
}

func (j *AsOfJoin[K, O, D]) rejectLocked(kind AsOfJoinErrorKind, side AsOfSide,
	key any, eventTime time.Time, input, existing any) AsOfJoinError {
	j.stats.Rejected++
	return AsOfJoinError{
		Kind:     kind,
		Side:     side,
		Key:      key,
		Time:     eventTime,
		Input:    input,
		Existing: existing,
	}
}

// finalizeLocked emits orders whose event time has been passed by both
// watermarks, or all pending orders when force is true.
func (j *AsOfJoin[K, O, D]) finalizeLocked(force bool) []AsOfJoinResult[K, O, D] {
	if !force && !j.finalizableLocked() {
		return nil
	}

	ready := make([]asOfPendingOrder[K, O], 0, len(j.pending))
	kept := j.pending[:0]
	for _, order := range j.pending {
		if force || j.orderReadyLocked(order.time) {
			ready = append(ready, order)
		} else {
			kept = append(kept, order)
		}
	}
	j.pending = kept

	sort.Slice(ready, func(i, k int) bool {
		if ready[i].time.Equal(ready[k].time) {
			return ready[i].seq < ready[k].seq
		}
		return ready[i].time.Before(ready[k].time)
	})

	results := make([]AsOfJoinResult[K, O, D], 0, len(ready))
	for _, order := range ready {
		results = append(results, j.resolveLocked(order))
	}
	return results
}

// finalizableLocked reports whether at least one pending order could become
// ready with the current watermarks.
func (j *AsOfJoin[K, O, D]) finalizableLocked() bool {
	if len(j.pending) == 0 {
		return false
	}
	orderProgress := j.progress[AsOfSideOrder]
	dimensionProgress := j.progress[AsOfSideDimension]
	return orderProgress.ended || dimensionProgress.ended ||
		(orderProgress.hasWatermark && dimensionProgress.hasWatermark)
}

func (j *AsOfJoin[K, O, D]) orderReadyLocked(eventTime time.Time) bool {
	return j.crossedLocked(AsOfSideOrder, eventTime) &&
		j.crossedLocked(AsOfSideDimension, eventTime)
}

func (j *AsOfJoin[K, O, D]) crossedLocked(side AsOfSide, eventTime time.Time) bool {
	progress := j.progress[side]
	if progress.ended {
		return true
	}
	return progress.hasWatermark && progress.watermark.After(eventTime)
}

// resolveLocked finds the latest dimension version at or before the order
// time and builds the result.
func (j *AsOfJoin[K, O, D]) resolveLocked(
	order asOfPendingOrder[K, O],
) AsOfJoinResult[K, O, D] {
	j.stats.Finalized++

	result := AsOfJoinResult[K, O, D]{
		Key:       order.key,
		Order:     order.order,
		OrderTime: order.time,
	}

	versions := j.dimensions[order.key]
	position := sort.Search(len(versions), func(i int) bool {
		return versions[i].time.After(order.time)
	}) - 1
	if position < 0 {
		j.stats.Unmatched++
		return result
	}

	version := versions[position]
	result.DimensionTime = version.time
	if version.deleted {
		result.Deleted = true
		j.stats.Deleted++
		return result
	}
	result.Dimension = version.value
	result.HasDimension = true
	j.stats.Matched++
	return result
}

// asOfQueue is an unbounded, thread-safe queue exposed as a channel. It
// prevents the join from blocking when late or rejected records are produced
// but their outputs are not being consumed yet.
type asOfQueue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []T
	head   int
	closed bool
	out    chan T
}

func newAsOfQueue[T any]() *asOfQueue[T] {
	queue := &asOfQueue[T]{out: make(chan T)}
	queue.cond = sync.NewCond(&queue.mu)
	go queue.run()
	return queue
}

func (q *asOfQueue[T]) push(item T) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, item)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *asOfQueue[T]) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Signal()
	q.mu.Unlock()
}

func (q *asOfQueue[T]) run() {
	defer close(q.out)
	for {
		q.mu.Lock()
		for q.head == len(q.items) && !q.closed {
			q.cond.Wait()
		}
		if q.head == len(q.items) && q.closed {
			q.mu.Unlock()
			return
		}
		item := q.items[q.head]
		var zero T
		q.items[q.head] = zero
		q.head++
		if q.head == len(q.items) {
			q.items = q.items[:0]
			q.head = 0
		}
		q.mu.Unlock()
		q.out <- item
	}
}
