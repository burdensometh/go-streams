package flow_test

import (
	"sync/atomic"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

// discardable is a test element that counts its discard notifications.
type discardable struct {
	value     int
	discarded atomic.Int32
}

func (d *discardable) Discard() {
	d.discarded.Add(1)
}

func TestFilter_DiscardsRejectedElements(t *testing.T) {
	in := make(chan any, 4)
	out := make(chan any, 4)

	elements := []*discardable{
		{value: 1},
		{value: 2},
		{value: 3},
		{value: 4},
	}
	for _, element := range elements {
		in <- element
	}
	close(in)

	source := ext.NewChanSource(in)
	sink := ext.NewChanSink(out)
	filter := flow.NewFilter(func(element *discardable) bool {
		return element.value%2 != 0
	}, 2)

	source.Via(filter).To(sink)

	// only the rejected elements are discarded: the even values
	assert.Equal(t, int32(0), elements[0].discarded.Load())
	assert.Equal(t, int32(1), elements[1].discarded.Load())
	assert.Equal(t, int32(0), elements[2].discarded.Load())
	assert.Equal(t, int32(1), elements[3].discarded.Load())

	output := readSlicePtr[discardable](out)
	assert.Equal(t, 2, len(output))
}

func TestThrottler_DiscardsDroppedElements(t *testing.T) {
	in := make(chan any, 8)
	out := make(chan any, 8)

	source := ext.NewChanSource(in)
	// a small buffer in discard mode drops the elements that the downstream
	// can not accept immediately
	throttler := flow.NewThrottler(2, 10*time.Millisecond, 1, flow.Discard)
	sink := ext.NewChanSink(out)

	var elements []*discardable
	for value := 0; value < 8; value++ {
		element := &discardable{value: value}
		elements = append(elements, element)
		in <- element
	}
	close(in)

	source.Via(throttler).To(sink)

	output := readSlicePtr[discardable](out)

	var discarded int32
	for _, element := range elements {
		discarded += element.discarded.Load()
	}
	// every element is either forwarded or explicitly discarded, so the
	// acknowledgment chain of the source is never stalled.
	assert.Equal(t, len(elements), len(output)+int(discarded))
	assert.Equal(t, int32(len(elements)-len(output)), discarded)
}
