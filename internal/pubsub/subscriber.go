package pubsub

import (
	"sync"
)

// Subscriber represents a client connection subscribed to channels/patterns.
type Subscriber struct {
	mu       sync.RWMutex
	channels map[string]struct{} // subscribed channel names
	patterns map[string]struct{} // subscribed pattern names
	msgCh    chan *Message       // message delivery channel
	closed   bool
}

func NewSubscriber(bufferSize int) *Subscriber {
	if bufferSize <= 0 {
		bufferSize = 1000 // default buffer size
	}
	return &Subscriber{
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
		msgCh:    make(chan *Message, bufferSize),
	}
}

// Messages returns the channel for receiving messages.
func (s *Subscriber) Messages() <-chan *Message {
	return s.msgCh
}

// deliver sends a message to the subscriber (non-blocking).
// Returns true if delivered, false if subscriber is slow/full.
func (s *Subscriber) deliver(msg *Message) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	select {
	case s.msgCh <- msg:
		return true
	default:
		// Channel full - drop message (subscriber too slow)
		return false
	}
}

func (s *Subscriber) addChannel(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[name] = struct{}{}
}

func (s *Subscriber) removeChannel(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.channels, name)
}

func (s *Subscriber) addPattern(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patterns[name] = struct{}{}
}

func (s *Subscriber) removePattern(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.patterns, name)
}

// GetChannels returns all subscribed channel names.
func (s *Subscriber) GetChannels() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	channels := make([]string, 0, len(s.channels))
	for ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels
}

// GetPatterns returns all subscribed pattern names.
func (s *Subscriber) GetPatterns() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	patterns := make([]string, 0, len(s.patterns))
	for pat := range s.patterns {
		patterns = append(patterns, pat)
	}
	return patterns
}

// CountTotal returns the total number of subscriptions (channels + patterns).
func (s *Subscriber) CountTotal() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels) + len(s.patterns)
}

func (s *Subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		close(s.msgCh)
	}
}

// IsClosed returns true if the subscriber is closed.
func (s *Subscriber) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}
