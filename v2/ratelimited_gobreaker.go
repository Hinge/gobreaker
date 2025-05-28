package gobreaker

import (
	"fmt"
	"sync"
	"time"
)

// RateLimitedCircuitBreaker is a state machine to prevent sending requests that are likely to fail.
// It differs from a regular CircuitBreaker in that it has a half-open state that:
// 1. Allows a rate-limited number of requests to pass through, rejecting requests when the limit is reached
// 2. Resets the state to closed after a specified timeout
// 3. Uses the same tripping logic as the Closed state.
type RateLimitedCircuitBreaker[T any] struct {
	name            string
	maxRequests     uint32
	interval        time.Duration
	timeout         time.Duration
	halfOpenTimeout time.Duration
	readyToTrip     func(counts Counts) bool
	isSuccessful    func(err error) bool
	onStateChange   func(name string, from State, to State)

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time

	inFlightRequests uint32
}

// RateLimitedCircuitBreakerSettings configures RateLimitedCircuitBreaker:
//
// Settings is the base configuration for the RateLimitedCircuitBreaker.
//
// HalfOpenTimeout is the period of the halg-open state,
// after which the state of the RateLimitedCircuitBreaker becomes closed.
// If HalfOpenTimeout is less than or equal to 0, the timeout value of the RateLimitedCircuitBreaker is set to 60 seconds.
type RateLimitedCircuitBreakerSettings struct {
	Settings

	HalfOpenTimeout time.Duration
}

// NewRateLimitedCircuitBreaker returns a new RateLimitedCircuitBreaker configured with the given Settings.
func NewRateLimitedCircuitBreaker[T any](st RateLimitedCircuitBreakerSettings) *RateLimitedCircuitBreaker[T] {
	cb := new(RateLimitedCircuitBreaker[T])

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.HalfOpenTimeout <= 0 {
		cb.halfOpenTimeout = defaultTimeout
	} else {
		cb.halfOpenTimeout = st.HalfOpenTimeout
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.toNewGeneration(time.Now())

	return cb
}

// Name returns the name of the RateLimitedCircuitBreaker.
func (cb *RateLimitedCircuitBreaker[T]) Name() string {
	return cb.name
}

// State returns the current state of the RateLimitedCircuitBreaker.
func (cb *RateLimitedCircuitBreaker[T]) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *RateLimitedCircuitBreaker[T]) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute runs the given request if the RateLimitedCircuitBreaker accepts it.
// Execute returns an error instantly if the RateLimitedCircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the RateLimitedCircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *RateLimitedCircuitBreaker[T]) Execute(req func() (T, error)) (T, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		var defaultValue T
		return defaultValue, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
}

func (cb *RateLimitedCircuitBreaker[T]) String() string {
	if cb == nil {
		return "RateLimitedCircuitBreaker(nil)"
	}

	return fmt.Sprintf("RateLimitedCircuitBreaker{name: %s, maxRequests: %d, interval: %s, timeout: %s, halfOpenTimeout: %s, state: %s, generation: %d, counts: %v, expiry: %s, inFlightRequests: %d}",
		cb.name, cb.maxRequests, cb.interval, cb.timeout, cb.halfOpenTimeout, cb.state, cb.generation, cb.counts, cb.expiry, cb.inFlightRequests)
}

func (cb *RateLimitedCircuitBreaker[T]) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.maxRequests > 0 && cb.inFlightRequests >= cb.maxRequests {
		// If there's already too many in-flight requests in HalfOpen state, reject the request
		return generation, ErrTooManyRequests
	}

	cb.counts.onRequest()
	cb.inFlightRequests++
	return generation, nil
}

func (cb *RateLimitedCircuitBreaker[T]) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.inFlightRequests--

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *RateLimitedCircuitBreaker[T]) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onSuccess()
	case StateHalfOpen:
		cb.counts.onSuccess()
		cb.currentState(now) // Refresh the state after a successful request in HalfOpen state
	}
}

func (cb *RateLimitedCircuitBreaker[T]) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed, StateHalfOpen:
		cb.counts.onFailure()
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	}
}

func (cb *RateLimitedCircuitBreaker[T]) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateHalfOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateClosed, now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

func (cb *RateLimitedCircuitBreaker[T]) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *RateLimitedCircuitBreaker[T]) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	case StateHalfOpen:
		cb.expiry = now.Add(cb.halfOpenTimeout)
	default:
		cb.expiry = zero
	}
}
