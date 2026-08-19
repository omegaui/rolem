package rolem

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Limiter struct {
	ctx    context.Context
	config *Config
	wg     *sync.WaitGroup
}

func NewLimiter(ctx context.Context, config *Config) *Limiter {
	var wg sync.WaitGroup
	limiter := &Limiter{ctx: ctx, config: config, wg: &wg}
	startZoneTickers(limiter)
	return limiter
}

func startZoneTickers(limiter *Limiter) {
	for i := 0; i < len(limiter.config.Zones); i++ {
		zone := &limiter.config.Zones[i]
		zone.Hits = make(AccessCache)
		zone.Passes = make(AccessCache)
		zone.Ticker = time.NewTicker(zone.Tick)
		go func(zone *Zone) {
			defer zone.Ticker.Stop()
			for {
				select {
				case <-limiter.ctx.Done():
					return
				case <-zone.Ticker.C:
					resetAccessCache(zone)
				}
			}
		}(zone)
	}
}

func resetAccessCache(zone *Zone) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.Hits = make(AccessCache)
	zone.Passes = make(AccessCache)
}

func updateZoneState(zone *Zone, state ZoneState) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.State = state
}

func zoneState(zone *Zone) ZoneState {
	zone.mu.RLock()
	defer zone.mu.RUnlock()
	return zone.State
}

// bump increments id's counter in cache, creating it when absent, and returns
// the new count. Callers must hold zone.mu for writing.
func bump(cache AccessCache, id string) int64 {
	counter := cache[id]
	if counter == nil {
		counter = new(int64)
		cache[id] = counter
	}
	return atomic.AddInt64(counter, 1)
}

func recordHit(zone *Zone, id string) int64 {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	return bump(zone.Hits, id)
}

func recordPass(zone *Zone, id string) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	bump(zone.Passes, id)
}

// readHits reports id's hit count, and whether it is still being tracked at
// all — a tick wipes the cache, which drops the entry.
func readHits(zone *Zone, id string) (int64, bool) {
	zone.mu.RLock()
	defer zone.mu.RUnlock()
	counter := zone.Hits[id]
	if counter == nil {
		return 0, false
	}
	return atomic.LoadInt64(counter), true
}

// beginBurst moves the zone into its bursting phase and hands the caller the
// burst timer. It reports false when the zone is already bursting, so exactly
// one caller ever owns the timer.
func beginBurst(zone *Zone) (*time.Timer, bool) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	if zone.State == Bursting || zone.BurstTimer != nil {
		return nil, false
	}
	zone.BurstTimer = time.NewTimer(zone.BurstPeriod)
	zone.State = Bursting
	return zone.BurstTimer, true
}

func endBurst(zone *Zone) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.BurstTimer = nil
}

// beginThrottle moves the zone into its throttling phase and hands the caller
// the throttle timer.
func beginThrottle(zone *Zone) *time.Timer {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.ThrottleTimer = time.NewTimer(zone.Throttle)
	zone.State = Throttling
	return zone.ThrottleTimer
}

// enterThrottle registers the caller as a waiter if the zone is still
// throttling, and reports whether it has to wait. Registering under the same
// lock that ends the phase means a caller either gets counted by endThrottle
// or observes the phase already over — it can never be left waiting for a
// wake-up nobody will send.
func enterThrottle(zone *Zone) bool {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	if zone.State != Throttling {
		return false
	}
	if zone.ThrottledCount == nil {
		zone.ThrottledCount = new(int64)
	}
	atomic.AddInt64(zone.ThrottledCount, 1)
	return true
}

// endThrottle returns the zone to normal and reports how many waiters have to
// be released.
func endThrottle(zone *Zone) int64 {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.State = Normal
	zone.ThrottleTimer = nil
	if zone.ThrottledCount == nil {
		return 0
	}
	return atomic.SwapInt64(zone.ThrottledCount, 0)
}

func (l *Limiter) Allow(zoneID string, id string) (bool, string) {
	zone, err := l.config.GetZone(zoneID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Allow ran into an error: %s", err.Error())
		return false, "some error"
	}

	// Name, Tick, Max, Burst, Backpressure and friends are set at construction
	// and never written afterwards, so they are read without the lock.
	if zone.Backpressure == IgnoreBackpressure {
		if zoneState(zone) == Throttling {
			return false, "backpressure ignored"
		}
	} else if enterThrottle(zone) {
		fmt.Println("Waiting ")

		select {
		case <-l.ctx.Done():
			return false, "cancelled"
		case <-zone.ThrottlePhaseOver:
		}

		resetAccessCache(zone)
	}

	hits := recordHit(zone, id)

	state := zoneState(zone)
	max := zone.Max
	if state == Bursting {
		max += zone.Burst
	}

	pass := hits <= max
	reason := "under limit"
	if !pass {
		if state == Bursting {
			reason = "hit burst limit"
		} else {
			reason = "hit max limit"
		}
	}
	if !pass && state != Bursting {
		if timer, started := beginBurst(zone); started {
			// check once more with burst
			burstMax := zone.Max + zone.Burst
			pass = hits <= burstMax
			if !pass {
				reason = "hit burst limit"
			}
			l.wg.Go(func() {
				l.watchBurst(zone, id, timer, burstMax)
			})
		}
	}

	// for metrics
	if pass {
		recordPass(zone, id)
	}
	return pass, reason
}

// watchBurst ends the burst phase once the burst period is over, dropping the
// zone into throttling if traffic is still above the burst ceiling.
func (l *Limiter) watchBurst(zone *Zone, id string, timer *time.Timer, burstMax int64) {
	select {
	case <-l.ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}
	endBurst(zone)

	// checking if hits are still increasing
	hits, tracked := readHits(zone, id)
	if !tracked || hits <= burstMax {
		// a tick has passed, or traffic settled back down
		updateZoneState(zone, Normal)
		return
	}

	// throttling phase
	throttleTimer := beginThrottle(zone)
	l.wg.Go(func() {
		l.watchThrottle(zone, throttleTimer)
	})
}

// watchThrottle ends the throttling phase and releases everyone parked in
// Allow waiting on it.
func (l *Limiter) watchThrottle(zone *Zone, timer *time.Timer) {
	select {
	case <-l.ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}

	waiting := endThrottle(zone)
	for range waiting {
		select {
		case <-l.ctx.Done():
			return
		case zone.ThrottlePhaseOver <- true:
		}
	}
	if waiting > 0 {
		fmt.Println("Resetting throttle count")
	}
}

func (l *Limiter) Wait() {
	l.wg.Wait()
}
