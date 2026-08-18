package rolem

import (
	"sync"
	"time"
)

const (
	IgnoreBackpressure = "backpressure rejected"
	DelayBackpressure  = "backpressure throttled"
)

type AccessCache map[string]*int64

type Zone struct {
	mu                sync.RWMutex
	Name              string
	Tick              time.Duration
	Max               int64
	Burst             int64
	BurstPeriod       time.Duration
	Bursting          bool
	Backpressure      string
	Throttle          time.Duration
	Throttling        bool
	ThrottledCount    *int64
	ThrottlePhaseOver chan bool
	Ticker            *time.Ticker
	BurstTimer        *time.Timer
	ThrottleTimer     *time.Timer
	Hits              AccessCache
	Passes            AccessCache
}

type Config struct {
	Zones []Zone
}

func (c *Config) GetZone(id string) (*Zone, error) {
	for i := 0; i < len(c.Zones); i++ {
		if c.Zones[i].Name == id {
			return &c.Zones[i], nil
		}
	}
	return nil, ZoneNotFound
}

func NewConfig(zones []Zone) *Config {
	config := Config{
		Zones: zones,
	}
	return &config
}
