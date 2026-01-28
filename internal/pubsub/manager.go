package pubsub

import (
	"context"
	"sync"
	"sync/atomic"
)

// Manager handles pub/sub operations with zero-copy message delivery.
type Manager struct {
	mu          sync.RWMutex
	channels    map[string]*channel      // channel name -> subscribers
	patterns    map[string]*patternSub   // pattern -> subscribers
	subscribers map[*Subscriber]struct{} // all active subscribers
	msgPool     sync.Pool                // message buffer pool
	totalMsgs   int64                    // stats: total messages published
	totalSubs   int64                    // stats: total active subscriptions
}

type channel struct {
	name        string
	subscribers map[*Subscriber]struct{}
}

type patternSub struct {
	pattern     string
	subscribers map[*Subscriber]struct{}
}

// Message represents a pub/sub message.
type Message struct {
	Channel string
	Payload []byte
	Pattern string // non-empty for pattern subscriptions
}

func NewManager() *Manager {
	return &Manager{
		channels:    make(map[string]*channel),
		patterns:    make(map[string]*patternSub),
		subscribers: make(map[*Subscriber]struct{}),
		msgPool: sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// Subscribe adds a subscriber to one or more channels.
func (m *Manager) Subscribe(sub *Subscriber, channels ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscribers[sub] = struct{}{}

	for _, chName := range channels {
		ch, ok := m.channels[chName]
		if !ok {
			ch = &channel{
				name:        chName,
				subscribers: make(map[*Subscriber]struct{}),
			}
			m.channels[chName] = ch
		}

		if _, exists := ch.subscribers[sub]; !exists {
			ch.subscribers[sub] = struct{}{}
			sub.addChannel(chName)
			atomic.AddInt64(&m.totalSubs, 1)
		}
	}

	return sub.GetChannels()
}

// PSubscribe adds a subscriber to one or more patterns.
func (m *Manager) PSubscribe(sub *Subscriber, patterns ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscribers[sub] = struct{}{}

	for _, patName := range patterns {
		pat, ok := m.patterns[patName]
		if !ok {
			pat = &patternSub{
				pattern:     patName,
				subscribers: make(map[*Subscriber]struct{}),
			}
			m.patterns[patName] = pat
		}

		if _, exists := pat.subscribers[sub]; !exists {
			pat.subscribers[sub] = struct{}{}
			sub.addPattern(patName)
			atomic.AddInt64(&m.totalSubs, 1)
		}
	}

	return sub.GetPatterns()
}

// Unsubscribe removes a subscriber from channels (all if none specified).
func (m *Manager) Unsubscribe(sub *Subscriber, channels ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(channels) == 0 {
		// Unsubscribe from all channels
		channels = sub.GetChannels()
	}

	for _, chName := range channels {
		ch, ok := m.channels[chName]
		if !ok {
			continue
		}

		if _, exists := ch.subscribers[sub]; exists {
			delete(ch.subscribers, sub)
			sub.removeChannel(chName)
			atomic.AddInt64(&m.totalSubs, -1)

			// Clean up empty channels
			if len(ch.subscribers) == 0 {
				delete(m.channels, chName)
			}
		}
	}

	// Clean up subscriber if no subscriptions left
	if sub.CountTotal() == 0 {
		delete(m.subscribers, sub)
	}

	return sub.GetChannels()
}

// PUnsubscribe removes a subscriber from patterns (all if none specified).
func (m *Manager) PUnsubscribe(sub *Subscriber, patterns ...string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(patterns) == 0 {
		// Unsubscribe from all patterns
		patterns = sub.GetPatterns()
	}

	for _, patName := range patterns {
		pat, ok := m.patterns[patName]
		if !ok {
			continue
		}

		if _, exists := pat.subscribers[sub]; exists {
			delete(pat.subscribers, sub)
			sub.removePattern(patName)
			atomic.AddInt64(&m.totalSubs, -1)

			// Clean up empty patterns
			if len(pat.subscribers) == 0 {
				delete(m.patterns, patName)
			}
		}
	}

	// Clean up subscriber if no subscriptions left
	if sub.CountTotal() == 0 {
		delete(m.subscribers, sub)
	}

	return sub.GetPatterns()
}

// Publish sends a message to all subscribers of a channel.
// Returns the number of subscribers that received the message.
func (m *Manager) Publish(ctx context.Context, channel string, payload []byte) int64 {
	m.mu.RLock()

	// Collect channel subscribers
	var subs []*Subscriber
	if ch, ok := m.channels[channel]; ok {
		subs = make([]*Subscriber, 0, len(ch.subscribers))
		for sub := range ch.subscribers {
			subs = append(subs, sub)
		}
	}

	// Collect pattern subscribers
	for patName, pat := range m.patterns {
		if matchPattern(patName, channel) {
			for sub := range pat.subscribers {
				// Avoid duplicates
				alreadyAdded := false
				for _, s := range subs {
					if s == sub {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					subs = append(subs, sub)
				}
			}
		}
	}

	m.mu.RUnlock()

	if len(subs) == 0 {
		return 0
	}

	// Create message (reuse from pool)
	msg := m.msgPool.Get().(*Message)
	msg.Channel = channel
	msg.Payload = payload
	msg.Pattern = ""

	// Broadcast to all subscribers (non-blocking)
	count := int64(0)
	for _, sub := range subs {
		if sub.deliver(msg) {
			count++
		}
	}

	atomic.AddInt64(&m.totalMsgs, 1)

	// Return message to pool after a delay (subscribers need time to read it)
	go func() {
		<-ctx.Done()
		m.msgPool.Put(msg)
	}()

	return count
}

// RemoveSubscriber completely removes a subscriber (called on disconnect).
func (m *Manager) RemoveSubscriber(sub *Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from all channels
	for _, chName := range sub.GetChannels() {
		if ch, ok := m.channels[chName]; ok {
			delete(ch.subscribers, sub)
			atomic.AddInt64(&m.totalSubs, -1)
			if len(ch.subscribers) == 0 {
				delete(m.channels, chName)
			}
		}
	}

	// Remove from all patterns
	for _, patName := range sub.GetPatterns() {
		if pat, ok := m.patterns[patName]; ok {
			delete(pat.subscribers, sub)
			atomic.AddInt64(&m.totalSubs, -1)
			if len(pat.subscribers) == 0 {
				delete(m.patterns, patName)
			}
		}
	}

	delete(m.subscribers, sub)
	sub.close()
}

// GetChannels returns list of active channels matching pattern (or all if "*").
func (m *Manager) GetChannels(pattern string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	channels := make([]string, 0, len(m.channels))
	for chName := range m.channels {
		if pattern == "" || pattern == "*" || matchPattern(pattern, chName) {
			channels = append(channels, chName)
		}
	}
	return channels
}

// GetNumSub returns subscriber count for each channel.
func (m *Manager) GetNumSub(channels ...string) map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]int64, len(channels))
	for _, chName := range channels {
		if ch, ok := m.channels[chName]; ok {
			result[chName] = int64(len(ch.subscribers))
		} else {
			result[chName] = 0
		}
	}
	return result
}

// GetNumPat returns total number of pattern subscriptions.
func (m *Manager) GetNumPat() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := int64(0)
	for _, pat := range m.patterns {
		count += int64(len(pat.subscribers))
	}
	return count
}

// Stats returns pub/sub statistics.
func (m *Manager) Stats() (totalMessages, totalSubscriptions int64) {
	return atomic.LoadInt64(&m.totalMsgs), atomic.LoadInt64(&m.totalSubs)
}
