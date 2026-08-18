package rolem

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Limiter struct {
	ctx    context.Context
	config *Config
}

func NewLimiter(ctx context.Context, config *Config) *Limiter {
	limiter := &Limiter{ctx: ctx, config: config}
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
	zone.mu.RLock()
	defer zone.mu.RUnlock()
	zone.Hits = nil
	zone.Passes = nil
	zone.Hits = make(AccessCache)
	zone.Passes = make(AccessCache)
}

func (l *Limiter) Allow(zoneID string, id string) (bool, string) {
	zone, err := l.config.GetZone(zoneID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Allow ran into an error: %s", err.Error())
		return false, "some error"
	}
	if zone.Throttling {
		if zone.Backpressure == IgnoreBackpressure {
			return false, "backpressure ignored"
		}
		if zone.ThrottledCount == nil {
			var firstThrottle int64 = 0
			zone.ThrottledCount = &firstThrottle
			atomic.StoreInt64(zone.ThrottledCount, 1)
		} else {
			atomic.AddInt64(zone.ThrottledCount, 1)
		}
		fmt.Println("Waiting ")
		<-zone.ThrottlePhaseOver
		resetAccessCache(zone)
	}

	value := zone.Hits[id]
	if value == nil {
		var firstHit int64 = 0
		zone.Hits[id] = &firstHit
	} else {
		atomic.AddInt64(value, 1)
	}
	value = zone.Hits[id]

	max := zone.Max
	if zone.Bursting {
		max += zone.Burst
	}

	pass := atomic.LoadInt64(value) < max
	reason := "under limit"
	if !pass {
		if zone.Bursting {
			reason = "hit burst limit"
		} else {
			reason = "hit max limit"
		}
	}
	if !pass {
		if !zone.Bursting {
			zone.BurstTimer = time.NewTimer(zone.BurstPeriod)
			zone.Bursting = true

			// check once more with burst
			max := zone.Max + zone.Burst
			pass = atomic.LoadInt64(value) < max
			if !pass {
				reason = "hit burst limit"
			}

			go func() {
				for {
					select {
					case <-l.ctx.Done():
						return
					case <-zone.BurstTimer.C:
						zone.Bursting = false
						zone.BurstTimer = nil

						// checking if hits are still increasing
						value := zone.Hits[id]
						if value == nil {
							// a tick has passed
							return
						}
						if atomic.LoadInt64(value) > max {
							// throttling phase
							zone.Throttling = true
							go func() {
								zone.ThrottleTimer = time.NewTimer(zone.Throttle)
								for {
									select {
									case <-l.ctx.Done():
										return
									case <-zone.ThrottleTimer.C:
										zone.Throttling = false
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
						}
						return
					}
				}
			}()
		}
	}

	// for metrics
	if pass {
		zone.mu.RLock()
		defer zone.mu.RUnlock()
		value := zone.Passes[id]
		if value == nil {
			var firstPass int64 = 0
			zone.Passes[id] = &firstPass
		} else {
			atomic.AddInt64(value, 1)
		}
	}
	return pass, reason
}
