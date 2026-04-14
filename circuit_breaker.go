package subscriber

import (
	"time"

	"github.com/fujiwara/mqbridge"
	expirable "github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	// DefaultCircuitBreakerMaxEntries is the maximum number of message keys tracked
	// by a single circuit breaker instance (LRU eviction when exceeded).
	DefaultCircuitBreakerMaxEntries = 1024

	// DefaultCircuitBreakerTTL is the default TTL for circuit breaker entries.
	DefaultCircuitBreakerTTL = 10 * time.Minute
)

// CircuitBreaker tracks per-message error counts and drops messages
// after a configured threshold is reached.
// Thread-safe via expirable.LRU (internally synchronized).
type CircuitBreaker struct {
	counts    *expirable.LRU[string, int]
	threshold int
}

// NewCircuitBreaker creates a CircuitBreaker with the given error threshold and TTL.
func NewCircuitBreaker(threshold int, ttl time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		counts:    expirable.NewLRU[string, int](DefaultCircuitBreakerMaxEntries, nil, ttl),
		threshold: threshold,
	}
}

// RecordError increments the error count for the given key.
// Returns true if the threshold has been reached (message should be dropped).
func (cb *CircuitBreaker) RecordError(key string) bool {
	count, _ := cb.counts.Get(key)
	count++
	cb.counts.Add(key, count)
	return count >= cb.threshold
}

// Clear removes the error count for the given key (called on success).
func (cb *CircuitBreaker) Clear(key string) {
	cb.counts.Remove(key)
}

// messageKey returns a stable identifier for a queue message across redeliveries.
// For RabbitMQ, uses rabbitmq.message_id header (stable across redeliveries).
// For SimpleMQ, uses QueueMessage.ID (MessageId, stable across redeliveries).
// If no stable ID is found, falls back to QueueMessage.ID.
func messageKey(qmsg *QueueMessage) string {
	if qmsg.Message != nil {
		if id, ok := qmsg.Message.Headers[mqbridge.HeaderRabbitMQMessageID]; ok && id != "" {
			return id
		}
	}
	return qmsg.ID
}
