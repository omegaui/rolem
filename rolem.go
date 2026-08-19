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
		go func(zone *Zone) {
			zone.Ticker = time.NewTicker(zone.Tick)
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
	zone.Hits = nil
	zone.Passes = nil
	zone.Hits = make(AccessCache)
	zone.Passes = make(AccessCache)
}

func updateZoneState(zone *Zone, state ZoneState) {
	zone.mu.Lock()
	defer zone.mu.Unlock()
	zone.State = state
}

func (l *Limiter) Allow(zoneID string, id string) (bool, string) {
	zone, err := l.config.GetZone(zoneID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Allow ran into an error: %s", err.Error())
		return false, "some error"
	}
	if zone.State == Throttling {
		if zone.Backpressure == IgnoreBackpressure {
			return false, "backpressure ignored"
		}
		zone.mu.Lock()
		if zone.ThrottledCount == nil {
			counter := new(int64)
			zone.ThrottledCount = counter
			atomic.StoreInt64(zone.ThrottledCount, 1)
		}
		atomic.AddInt64(zone.ThrottledCount, 1)
		zone.mu.Unlock()

		fmt.Println("Waiting ")

		select {
		case <-l.ctx.Done():
			return false, "cancelled"
		case <-zone.ThrottlePhaseOver:
		}

		resetAccessCache(zone)
	}

	zone.mu.RLock()
	value := zone.Hits[id]
	if value == nil {
		counter := new(int64)
		zone.Hits[id] = counter
	}
	value = zone.Hits[id]
	atomic.AddInt64(value, 1)
	zone.mu.RUnlock()

	max := zone.Max
	if zone.State == Bursting {
		max += zone.Burst
	}

	pass := atomic.LoadInt64(value) <= max
	reason := "under limit"
	if !pass {
		if zone.State == Bursting {
			reason = "hit burst limit"
		} else {
			reason = "hit max limit"
		}
	}
	if !pass {
		if zone.State != Bursting && zone.BurstTimer == nil {
			zone.BurstTimer = time.NewTimer(zone.BurstPeriod)
			updateZoneState(zone, Bursting)

			// check once more with burst
			max := zone.Max + zone.Burst
			pass = atomic.LoadInt64(value) <= max
			if !pass {
				reason = "hit burst limit"
			}
			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				for {
					select {
					case <-l.ctx.Done():
						return
					case <-zone.BurstTimer.C:
						zone.BurstTimer = nil

						// checking if hits are still increasing
						value := zone.Hits[id]
						if value == nil {
							// a tick has passed
							updateZoneState(zone, Normal)
							return
						}
						hits := atomic.LoadInt64(value)
						if hits > max {
							// throttling phase
							updateZoneState(zone, Throttling)
							go func() {
								zone.ThrottleTimer = time.NewTimer(zone.Throttle)
								for {
									select {
									case <-l.ctx.Done():
										return
									case <-zone.ThrottleTimer.C:
										updateZoneState(zone, Normal)
										if zone.Backpressure == DelayBackpressure {
											var i int64 = 0
											for ; i < atomic.LoadInt64(zone.ThrottledCount); i++ {
												zone.ThrottlePhaseOver <- true
											}
											fmt.Println("Resetting throttle count")
											atomic.StoreInt64(zone.ThrottledCount, 0)
										}
										return
									}
								}
							}()
						} else {
							updateZoneState(zone, Normal)
						}
						return
					}
				}
			}()
		}
	}

	// for metrics
	if pass {
		zone.mu.Lock()
		defer zone.mu.Unlock()
		value := zone.Passes[id]
		if value == nil {
			counter := new(int64)
			zone.Passes[id] = counter
		}
		value = zone.Passes[id]
		atomic.AddInt64(value, 1)
	}
	return pass, reason
}

func (l *Limiter) Wait() {
	l.wg.Wait()
}
