package pubsub

import (
	"context"
	"testing"
	"time"
)

func TestManager_Subscribe(t *testing.T) {
	mgr := NewManager()
	sub := NewSubscriber(10)

	channels := mgr.Subscribe(sub, "news", "sports")

	if len(channels) != 2 {
		t.Errorf("Expected 2 channels, got %d", len(channels))
	}
}

func TestManager_Publish(t *testing.T) {
	mgr := NewManager()
	sub := NewSubscriber(10)

	mgr.Subscribe(sub, "news")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	count := mgr.Publish(ctx, "news", []byte("hello"))
	if count != 1 {
		t.Errorf("Expected 1 subscriber to receive message, got %d", count)
	}

	// Check message received
	select {
	case msg := <-sub.Messages():
		if msg.Channel != "news" {
			t.Errorf("Expected channel 'news', got '%s'", msg.Channel)
		}
		if string(msg.Payload) != "hello" {
			t.Errorf("Expected payload 'hello', got '%s'", string(msg.Payload))
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Timeout waiting for message")
	}
}

func TestManager_PatternSubscribe(t *testing.T) {
	mgr := NewManager()
	sub := NewSubscriber(10)

	mgr.PSubscribe(sub, "user:*")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	count := mgr.Publish(ctx, "user:123", []byte("login"))
	if count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}

	// Check message received
	select {
	case msg := <-sub.Messages():
		if msg.Channel != "user:123" {
			t.Errorf("Expected channel 'user:123', got '%s'", msg.Channel)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Timeout waiting for message")
	}
}

func TestManager_Unsubscribe(t *testing.T) {
	mgr := NewManager()
	sub := NewSubscriber(10)

	mgr.Subscribe(sub, "news", "sports")

	remaining := mgr.Unsubscribe(sub, "news")
	if len(remaining) != 1 {
		t.Errorf("Expected 1 remaining channel, got %d", len(remaining))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Should not receive on unsubscribed channel
	count := mgr.Publish(ctx, "news", []byte("test"))
	if count != 0 {
		t.Errorf("Expected 0 subscribers, got %d", count)
	}

	// Should still receive on subscribed channel
	count = mgr.Publish(ctx, "sports", []byte("test"))
	if count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}
}

func TestPatternMatching(t *testing.T) {
	tests := []struct {
		pattern string
		channel string
		match   bool
	}{
		{"*", "anything", true},
		{"user:*", "user:123", true},
		{"user:*", "user:", true},
		{"user:*", "admin:123", false},
		{"user:?", "user:1", true},
		{"user:?", "user:12", false},
		{"user:*:active", "user:123:active", true},
		{"user:*:active", "user:123:inactive", false},
		{"news", "news", true},
		{"news", "sports", false},
	}

	for _, tt := range tests {
		result := matchPattern(tt.pattern, tt.channel)
		if result != tt.match {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.channel, result, tt.match)
		}
	}
}

func TestManager_MultipleSubscribers(t *testing.T) {
	mgr := NewManager()
	sub1 := NewSubscriber(10)
	sub2 := NewSubscriber(10)

	mgr.Subscribe(sub1, "news")
	mgr.Subscribe(sub2, "news")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	count := mgr.Publish(ctx, "news", []byte("breaking"))
	if count != 2 {
		t.Errorf("Expected 2 subscribers, got %d", count)
	}
}

func TestManager_GetChannels(t *testing.T) {
	mgr := NewManager()
	sub := NewSubscriber(10)

	mgr.Subscribe(sub, "news", "sports", "weather")

	channels := mgr.GetChannels("*")
	if len(channels) != 3 {
		t.Errorf("Expected 3 channels, got %d", len(channels))
	}

	channels = mgr.GetChannels("news")
	if len(channels) != 1 {
		t.Errorf("Expected 1 channel, got %d", len(channels))
	}
}

func TestManager_GetNumSub(t *testing.T) {
	mgr := NewManager()
	sub1 := NewSubscriber(10)
	sub2 := NewSubscriber(10)

	mgr.Subscribe(sub1, "news")
	mgr.Subscribe(sub2, "news")
	mgr.Subscribe(sub1, "sports")

	counts := mgr.GetNumSub("news", "sports", "weather")

	if counts["news"] != 2 {
		t.Errorf("Expected 2 subscribers for 'news', got %d", counts["news"])
	}
	if counts["sports"] != 1 {
		t.Errorf("Expected 1 subscriber for 'sports', got %d", counts["sports"])
	}
	if counts["weather"] != 0 {
		t.Errorf("Expected 0 subscribers for 'weather', got %d", counts["weather"])
	}
}

func BenchmarkPublish_SingleSubscriber(b *testing.B) {
	mgr := NewManager()
	sub := NewSubscriber(10000)
	mgr.Subscribe(sub, "bench")

	ctx := context.Background()
	payload := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(ctx, "bench", payload)
	}
}

func BenchmarkPublish_100Subscribers(b *testing.B) {
	mgr := NewManager()

	for i := 0; i < 100; i++ {
		sub := NewSubscriber(10000)
		mgr.Subscribe(sub, "bench")
		// Drain messages
		go func(s *Subscriber) {
			for range s.Messages() {
			}
		}(sub)
	}

	ctx := context.Background()
	payload := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Publish(ctx, "bench", payload)
	}
}

func BenchmarkPatternMatching(b *testing.B) {
	pattern := "user:*:active"
	channel := "user:12345:active"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchPattern(pattern, channel)
	}
}
