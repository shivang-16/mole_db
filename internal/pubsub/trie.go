package pubsub

import "strings"

// PatternTrie stores pattern subscriptions in a trie for O(k) matching.
type PatternTrie struct {
	root *trieNode
}

type trieNode struct {
	children map[byte]*trieNode
	wildcard *trieNode // For '*' pattern
	patterns map[string]*patternSub
	isEnd    bool
}

func NewPatternTrie() *PatternTrie {
	return &PatternTrie{
		root: &trieNode{
			children: make(map[byte]*trieNode),
			patterns: make(map[string]*patternSub),
		},
	}
}

// Insert adds a pattern to the trie.
func (t *PatternTrie) Insert(pattern string, ps *patternSub) {
	node := t.root
	i := 0
	n := len(pattern)

	for i < n {
		ch := pattern[i]

		if ch == '*' {
			// Wildcard branch
			if node.wildcard == nil {
				node.wildcard = &trieNode{
					children: make(map[byte]*trieNode),
					patterns: make(map[string]*patternSub),
				}
			}
			node = node.wildcard
			i++

			// Skip consecutive wildcards
			for i < n && pattern[i] == '*' {
				i++
			}
		} else if ch == '?' {
			// Question mark matches single char - skip for now, handled in matching
			i++
		} else {
			// Regular character
			if node.children[ch] == nil {
				node.children[ch] = &trieNode{
					children: make(map[byte]*trieNode),
					patterns: make(map[string]*patternSub),
				}
			}
			node = node.children[ch]
			i++
		}
	}

	node.isEnd = true
	node.patterns[pattern] = ps
}

// Remove removes a pattern from the trie.
func (t *PatternTrie) Remove(pattern string) {
	t.removeHelper(t.root, pattern, 0)
}

func (t *PatternTrie) removeHelper(node *trieNode, pattern string, depth int) bool {
	if node == nil {
		return false
	}

	if depth == len(pattern) {
		delete(node.patterns, pattern)
		// Return true if node can be deleted (no patterns and no children)
		return len(node.patterns) == 0 && len(node.children) == 0 && node.wildcard == nil
	}

	ch := pattern[depth]

	if ch == '*' {
		if t.removeHelper(node.wildcard, pattern, depth+1) {
			node.wildcard = nil
		}
	} else if ch != '?' {
		if t.removeHelper(node.children[ch], pattern, depth+1) {
			delete(node.children, ch)
		}
	}

	return len(node.patterns) == 0 && len(node.children) == 0 && node.wildcard == nil
}

// MatchAll returns all patterns that match the given channel.
func (t *PatternTrie) MatchAll(channel string) map[string]*patternSub {
	result := make(map[string]*patternSub)
	t.matchHelper(t.root, channel, 0, result)
	return result
}

func (t *PatternTrie) matchHelper(node *trieNode, channel string, depth int, result map[string]*patternSub) {
	if node == nil {
		return
	}

	// If we've consumed the entire channel
	if depth == len(channel) {
		// Add all patterns at this node
		for pattern, ps := range node.patterns {
			if node.isEnd && matchPattern(pattern, channel) {
				result[pattern] = ps
			}
		}

		// Wildcard can match empty string
		if node.wildcard != nil {
			for pattern, ps := range node.wildcard.patterns {
				if matchPattern(pattern, channel) {
					result[pattern] = ps
				}
			}
		}
		return
	}

	ch := channel[depth]

	// Try exact character match
	if child, ok := node.children[ch]; ok {
		t.matchHelper(child, channel, depth+1, result)
	}

	// Try wildcard match (matches any sequence)
	if node.wildcard != nil {
		// Wildcard can match 0 or more characters
		for i := depth; i <= len(channel); i++ {
			t.matchHelper(node.wildcard, channel, i, result)
		}
	}
}

// FastMatch optimizes for common patterns like "prefix:*"
func FastMatchPrefix(patterns map[string]*patternSub, channel string) []*patternSub {
	result := make([]*patternSub, 0, 4)

	for patName, pat := range patterns {
		// Fast path for "prefix:*" patterns
		if strings.HasSuffix(patName, ":*") {
			prefix := patName[:len(patName)-2]
			if strings.HasPrefix(channel, prefix) {
				result = append(result, pat)
				continue
			}
		}

		// Fast path for exact match
		if patName == channel {
			result = append(result, pat)
			continue
		}

		// Fast path for "*" wildcard
		if patName == "*" {
			result = append(result, pat)
			continue
		}

		// Fallback to full pattern matching
		if matchPattern(patName, channel) {
			result = append(result, pat)
		}
	}

	return result
}
