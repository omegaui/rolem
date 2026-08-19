package rolem

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	GLOBAL = "global"
)

func TestConfigHeap(t *testing.T) {
	config := NewConfig(
		[]Zone{
			{
				Name:              GLOBAL,
				Tick:              time.Second,
				Max:               1,
				Burst:             3,
				BurstPeriod:       time.Second * 2,
				Backpressure:      DelayBackpressure,
				Throttle:          time.Second * 5,
				ThrottlePhaseOver: make(chan bool),
				Hits:              make(AccessCache),
				Passes:            make(AccessCache),
			},
		},
	)
	limiter := NewLimiter(context.Background(), config)

	var wg sync.WaitGroup
	var passes int64 = 0
	var failed int64 = 0
	for i := range 100 {
		time.Sleep(time.Millisecond * 2 * time.Duration(i))
		wg.Go(func() {
			result, reason := limiter.Allow(GLOBAL, "any")
			if result {
				atomic.AddInt64(&passes, 1)
			} else {
				atomic.AddInt64(&failed, 1)
			}
			fmt.Printf("Request(%d) done, result = %s, reason = %s\n", i + 1, strconv.FormatBool(result), reason)
		})
	}
	wg.Wait()
	limiter.Wait()
	fmt.Printf("%d, %d\n", passes, failed)
}
