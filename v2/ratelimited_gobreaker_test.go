package gobreaker

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var defaultRLCB *RateLimitedCircuitBreaker[bool]
var customRLCB *RateLimitedCircuitBreaker[bool]

type RLStateChange struct {
	name string
	from State
	to   State
}

var rlStateChange RLStateChange

func pseudoSleepRL(cb *RateLimitedCircuitBreaker[bool], period time.Duration) {
	if !cb.expiry.IsZero() {
		cb.expiry = cb.expiry.Add(-period)
	}
}

func succeedRL(cb *RateLimitedCircuitBreaker[bool]) error {
	_, err := cb.Execute(func() (bool, error) { return true, nil })
	return err
}

func succeedLaterRL(cb *RateLimitedCircuitBreaker[bool], delay time.Duration) <-chan error {
	ch := make(chan error)
	go func() {
		_, err := cb.Execute(func() (bool, error) {
			time.Sleep(delay)
			return true, nil
		})
		ch <- err
	}()
	return ch
}

func failRL(cb *RateLimitedCircuitBreaker[bool]) error {
	msg := "fail"
	_, err := cb.Execute(func() (bool, error) { return false, errors.New(msg) })
	if err.Error() == msg {
		return nil
	}
	return err
}

func causePanicRL(cb *RateLimitedCircuitBreaker[bool]) error {
	_, err := cb.Execute(func() (bool, error) { panic("oops"); return false, nil })
	return err
}

func newCustomRL() *RateLimitedCircuitBreaker[bool] {
	var customSt Settings
	customSt.Name = "cb"
	customSt.MaxRequests = 3
	customSt.Interval = time.Duration(30) * time.Second
	customSt.Timeout = time.Duration(90) * time.Second
	customSt.ReadyToTrip = func(counts Counts) bool {
		numReqs := counts.Requests
		failureRatio := float64(counts.TotalFailures) / float64(numReqs)

		counts.clear() // no effect on customRLCB.counts

		return numReqs >= 3 && failureRatio >= 0.6
	}
	customSt.OnStateChange = func(name string, from State, to State) {
		rlStateChange = RLStateChange{name, from, to}
	}

	var customRLSt RateLimitedCircuitBreakerSettings
	customRLSt.Settings = customSt
	customRLSt.HalfOpenTimeout = time.Duration(90) * time.Second

	return NewRateLimitedCircuitBreaker[bool](customRLSt)
}

func newNegativeDurationRLCB() *RateLimitedCircuitBreaker[bool] {
	var negativeSt Settings
	negativeSt.Name = "ncb"
	negativeSt.Interval = time.Duration(-30) * time.Second
	negativeSt.Timeout = time.Duration(-90) * time.Second

	var negativeRLSt RateLimitedCircuitBreakerSettings
	negativeRLSt.Settings = negativeSt
	negativeRLSt.HalfOpenTimeout = time.Duration(-90) * time.Second

	return NewRateLimitedCircuitBreaker[bool](negativeRLSt)
}

func init() {
	defaultRLCB = NewRateLimitedCircuitBreaker[bool](RateLimitedCircuitBreakerSettings{})
	customRLCB = newCustomRL()
}

func TestNewRateLimitedCircuitBreaker(t *testing.T) {
	defaultRLCB := NewRateLimitedCircuitBreaker[bool](RateLimitedCircuitBreakerSettings{})
	assert.Equal(t, "", defaultRLCB.name)
	assert.Equal(t, uint32(1), defaultRLCB.maxRequests)
	assert.Equal(t, time.Duration(0), defaultRLCB.interval)
	assert.Equal(t, time.Duration(60)*time.Second, defaultRLCB.timeout)
	assert.Equal(t, time.Duration(60)*time.Second, defaultRLCB.halfOpenTimeout)
	assert.NotNil(t, defaultRLCB.readyToTrip)
	assert.Nil(t, defaultRLCB.onStateChange)
	assert.Equal(t, StateClosed, defaultRLCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultRLCB.counts)
	assert.True(t, defaultRLCB.expiry.IsZero())

	customRLCB := newCustomRL()
	assert.Equal(t, "cb", customRLCB.name)
	assert.Equal(t, uint32(3), customRLCB.maxRequests)
	assert.Equal(t, time.Duration(30)*time.Second, customRLCB.interval)
	assert.Equal(t, time.Duration(90)*time.Second, customRLCB.timeout)
	assert.Equal(t, time.Duration(90)*time.Second, customRLCB.halfOpenTimeout)
	assert.NotNil(t, customRLCB.readyToTrip)
	assert.NotNil(t, customRLCB.onStateChange)
	assert.Equal(t, StateClosed, customRLCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customRLCB.counts)
	assert.False(t, customRLCB.expiry.IsZero())

	negativeDurationCB := newNegativeDurationRLCB()
	assert.Equal(t, "ncb", negativeDurationCB.name)
	assert.Equal(t, uint32(1), negativeDurationCB.maxRequests)
	assert.Equal(t, time.Duration(0)*time.Second, negativeDurationCB.interval)
	assert.Equal(t, time.Duration(60)*time.Second, negativeDurationCB.timeout)
	assert.Equal(t, time.Duration(60)*time.Second, negativeDurationCB.halfOpenTimeout)
	assert.NotNil(t, negativeDurationCB.readyToTrip)
	assert.Nil(t, negativeDurationCB.onStateChange)
	assert.Equal(t, StateClosed, negativeDurationCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, negativeDurationCB.counts)
	assert.True(t, negativeDurationCB.expiry.IsZero())
}

func TestDefaultRateLimitedCircuitBreaker(t *testing.T) {
	assert.Equal(t, "", defaultRLCB.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, failRL(defaultRLCB))
	}
	assert.Equal(t, StateClosed, defaultRLCB.State())
	assert.Equal(t, Counts{5, 0, 5, 0, 5}, defaultRLCB.counts)

	assert.Nil(t, succeedRL(defaultRLCB))
	assert.Equal(t, StateClosed, defaultRLCB.State())
	assert.Equal(t, Counts{6, 1, 5, 1, 0}, defaultRLCB.counts)

	assert.Nil(t, failRL(defaultRLCB))
	assert.Equal(t, StateClosed, defaultRLCB.State())
	assert.Equal(t, Counts{7, 1, 6, 0, 1}, defaultRLCB.counts)

	// StateClosed to StateOpen
	for i := 0; i < 5; i++ {
		assert.Nil(t, failRL(defaultRLCB)) // 6 consecutive failures
	}
	assert.Equal(t, StateOpen, defaultRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultRLCB.counts)
	assert.False(t, defaultRLCB.expiry.IsZero())

	assert.Error(t, succeedRL(defaultRLCB))
	assert.Error(t, failRL(defaultRLCB))
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultRLCB.counts)

	pseudoSleepRL(defaultRLCB, time.Duration(59)*time.Second)
	assert.Equal(t, StateOpen, defaultRLCB.State())

	// StateOpen to StateHalfOpen
	pseudoSleepRL(defaultRLCB, time.Duration(1)*time.Second) // over Timeout
	assert.Equal(t, StateHalfOpen, defaultRLCB.State())

	// Stay in StateHalfOpen
	assert.Nil(t, failRL(defaultRLCB))
	assert.Equal(t, StateHalfOpen, defaultRLCB.State())
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, defaultRLCB.counts)

	// StateHalfOpen to StateOpen
	for i := 0; i < 5; i++ {
		assert.Nil(t, failRL(defaultRLCB)) // add 5 more failures for a total of 6 consecutive failures
	}
	assert.Equal(t, StateOpen, defaultRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultRLCB.counts)
	assert.False(t, defaultRLCB.expiry.IsZero())

	// StateOpen to StateHalfOpen
	pseudoSleepRL(defaultRLCB, time.Duration(60)*time.Second) // over Timeout
	assert.Equal(t, StateHalfOpen, defaultRLCB.State())

	// StateHalfOpen to StateClosed
	pseudoSleepRL(defaultRLCB, time.Duration(60)*time.Second) // over HalfOpenTimeout
	assert.Equal(t, StateClosed, defaultRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultRLCB.counts)
	assert.True(t, defaultRLCB.expiry.IsZero())
}

func TestCustomRateLimitedCircuitBreaker(t *testing.T) {
	assert.Equal(t, "cb", customRLCB.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, succeedRL(customRLCB))
		assert.Nil(t, failRL(customRLCB))
	}
	assert.Equal(t, StateClosed, customRLCB.State())
	assert.Equal(t, Counts{10, 5, 5, 0, 1}, customRLCB.counts)

	pseudoSleepRL(customRLCB, time.Duration(29)*time.Second)
	assert.Nil(t, succeedRL(customRLCB))
	assert.Equal(t, StateClosed, customRLCB.State())
	assert.Equal(t, Counts{11, 6, 5, 1, 0}, customRLCB.counts)

	pseudoSleepRL(customRLCB, time.Duration(1)*time.Second) // over Interval
	assert.Nil(t, failRL(customRLCB))
	assert.Equal(t, StateClosed, customRLCB.State())
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, customRLCB.counts)

	// StateClosed to StateOpen
	assert.Nil(t, succeedRL(customRLCB))
	assert.Nil(t, failRL(customRLCB)) // failure ratio: 2/3 >= 0.6
	assert.Equal(t, StateOpen, customRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customRLCB.counts)
	assert.False(t, customRLCB.expiry.IsZero())
	assert.Equal(t, RLStateChange{"cb", StateClosed, StateOpen}, rlStateChange)

	// StateOpen to StateHalfOpen
	pseudoSleepRL(customRLCB, time.Duration(90)*time.Second)
	assert.Equal(t, StateHalfOpen, customRLCB.State())
	assert.Equal(t, RLStateChange{"cb", StateOpen, StateHalfOpen}, rlStateChange)

	assert.Nil(t, succeedRL(customRLCB))
	assert.Nil(t, succeedRL(customRLCB))
	assert.Equal(t, StateHalfOpen, customRLCB.State())
	assert.Equal(t, Counts{2, 2, 0, 2, 0}, customRLCB.counts)

	// Rate-limited while in StateHalfOpen
	ch1 := succeedLaterRL(customRLCB, time.Duration(100)*time.Millisecond)
	ch2 := succeedLaterRL(customRLCB, time.Duration(100)*time.Millisecond)
	ch3 := succeedLaterRL(customRLCB, time.Duration(100)*time.Millisecond) // 3 inflight requests
	time.Sleep(time.Duration(50) * time.Millisecond)
	assert.ErrorIs(t, succeedRL(customRLCB), ErrTooManyRequests) // over MaxRequests

	time.Sleep(time.Duration(100) * time.Millisecond)
	assert.Equal(t, Counts{5, 5, 0, 5, 0}, customRLCB.counts)
	assert.Nil(t, <-ch1)
	assert.Nil(t, <-ch2)
	assert.Nil(t, <-ch3)

	// StateHalfOpen to StateClosed
	pseudoSleepRL(customRLCB, time.Duration(90)*time.Second) // over HalfOpenTimeout
	assert.Equal(t, StateClosed, customRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customRLCB.counts)
	assert.False(t, customRLCB.expiry.IsZero())
	assert.Equal(t, RLStateChange{"cb", StateHalfOpen, StateClosed}, rlStateChange)
}

func TestPanicInRequestRL(t *testing.T) {
	assert.Panics(t, func() { _ = causePanicRL(defaultRLCB) })
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, defaultRLCB.counts)
}

func TestGenerationRL(t *testing.T) {
	pseudoSleepRL(customRLCB, time.Duration(29)*time.Second)
	assert.Nil(t, succeedRL(customRLCB))
	ch := succeedLaterRL(customRLCB, time.Duration(1500)*time.Millisecond)
	time.Sleep(time.Duration(500) * time.Millisecond)
	assert.Equal(t, Counts{2, 1, 0, 1, 0}, customRLCB.counts)

	time.Sleep(time.Duration(500) * time.Millisecond) // over Interval
	assert.Equal(t, StateClosed, customRLCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customRLCB.counts)

	// the request from the previous generation has no effect on customRLCB.counts
	assert.Nil(t, <-ch)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customRLCB.counts)
}

func TestCustomIsSuccessfulRL(t *testing.T) {
	isSuccessful := func(error) bool {
		return true
	}
	cb := NewRateLimitedCircuitBreaker[bool](RateLimitedCircuitBreakerSettings{
		Settings: Settings{
			IsSuccessful: isSuccessful,
		},
	})

	for i := 0; i < 5; i++ {
		assert.Nil(t, failRL(cb))
	}
	assert.Equal(t, StateClosed, cb.State())
	assert.Equal(t, Counts{5, 5, 0, 5, 0}, cb.counts)

	cb.counts.clear()

	cb.isSuccessful = func(err error) bool {
		return err == nil
	}
	for i := 0; i < 6; i++ {
		assert.Nil(t, failRL(cb))
	}
	assert.Equal(t, StateOpen, cb.State())

}

func TestRateLimitedCircuitBreakerInParallel(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	ch := make(chan error)

	const numReqs = 10000
	routine := func() {
		for i := 0; i < numReqs; i++ {
			ch <- succeedRL(customRLCB)
		}
	}

	const numRoutines = 10
	for i := 0; i < numRoutines; i++ {
		go routine()
	}

	total := uint32(numReqs * numRoutines)
	for i := uint32(0); i < total; i++ {
		err := <-ch
		assert.Nil(t, err)
	}
	assert.Equal(t, Counts{total, total, 0, total, 0}, customRLCB.counts)
}
