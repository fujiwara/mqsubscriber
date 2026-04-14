package subscriber

import (
	"testing"
	"time"

	"github.com/fujiwara/mqbridge"
)

func TestCircuitBreakerRecordError(t *testing.T) {
	cb := NewCircuitBreaker(3, 10*time.Minute)

	// First two errors should not trigger
	if cb.RecordError("msg-1") {
		t.Fatal("should not trigger after 1 error")
	}
	if cb.RecordError("msg-1") {
		t.Fatal("should not trigger after 2 errors")
	}
	// Third error should trigger
	if !cb.RecordError("msg-1") {
		t.Fatal("should trigger after 3 errors")
	}
	// Subsequent errors should continue to trigger
	if !cb.RecordError("msg-1") {
		t.Fatal("should still trigger after exceeding threshold")
	}
}

func TestCircuitBreakerClear(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Minute)

	cb.RecordError("msg-1")
	cb.Clear("msg-1")

	// After clear, count should reset
	if cb.RecordError("msg-1") {
		t.Fatal("should not trigger after clear and 1 error")
	}
	if !cb.RecordError("msg-1") {
		t.Fatal("should trigger after clear and 2 errors")
	}
}

func TestCircuitBreakerIndependentKeys(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Minute)

	cb.RecordError("msg-1")
	cb.RecordError("msg-2")

	// Each key tracks independently
	if !cb.RecordError("msg-1") {
		t.Fatal("msg-1 should trigger after 2 errors")
	}
	if !cb.RecordError("msg-2") {
		t.Fatal("msg-2 should trigger after 2 errors")
	}
}

func TestCircuitBreakerTTLExpiry(t *testing.T) {
	// Use a very short TTL
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordError("msg-1")
	time.Sleep(100 * time.Millisecond)

	// After TTL, the entry should have expired
	if cb.RecordError("msg-1") {
		t.Fatal("should not trigger after TTL expiry (count should be reset)")
	}
}

func TestMessageKey(t *testing.T) {
	tests := []struct {
		name string
		qmsg *QueueMessage
		want string
	}{
		{
			name: "SimpleMQ uses QueueMessage.ID",
			qmsg: &QueueMessage{
				ID: "simplemq-msg-123",
				Message: &mqbridge.Message{
					Headers: map[string]string{"type": "task"},
				},
			},
			want: "simplemq-msg-123",
		},
		{
			name: "RabbitMQ uses rabbitmq.message_id header",
			qmsg: &QueueMessage{
				ID: "42", // delivery tag (changes on redelivery)
				Message: &mqbridge.Message{
					Headers: map[string]string{
						"rabbitmq.message_id": "stable-rmq-id",
					},
				},
			},
			want: "stable-rmq-id",
		},
		{
			name: "RabbitMQ without message_id falls back to ID",
			qmsg: &QueueMessage{
				ID: "42",
				Message: &mqbridge.Message{
					Headers: map[string]string{},
				},
			},
			want: "42",
		},
		{
			name: "nil message falls back to ID",
			qmsg: &QueueMessage{
				ID:      "fallback-id",
				Message: nil,
			},
			want: "fallback-id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := messageKey(tt.qmsg)
			if got != tt.want {
				t.Errorf("messageKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
