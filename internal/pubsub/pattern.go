package pubsub

import "strings"

// matchPattern checks if a channel name matches a pattern.
// Supports:
//   - * matches any sequence of characters (including empty)
//   - ? matches exactly one character
//   - Literal characters must match exactly
func matchPattern(pattern, channel string) bool {
	// Fast path: exact match
	if pattern == channel {
		return true
	}

	// Fast path: wildcard "*" matches everything
	if pattern == "*" {
		return true
	}

	// No wildcards - must be exact match
	if !strings.ContainsAny(pattern, "*?") {
		return pattern == channel
	}

	// Match with wildcards
	return matchWildcard(pattern, channel, 0, 0)
}

// matchWildcard performs recursive wildcard matching.
func matchWildcard(pattern, str string, pi, si int) bool {
	pLen := len(pattern)
	sLen := len(str)

	for pi < pLen {
		switch pattern[pi] {
		case '*':
			// Skip consecutive stars
			for pi < pLen && pattern[pi] == '*' {
				pi++
			}

			// Star at end matches everything
			if pi == pLen {
				return true
			}

			// Try matching rest of pattern at each position
			for si <= sLen {
				if matchWildcard(pattern, str, pi, si) {
					return true
				}
				si++
			}
			return false

		case '?':
			// ? must match exactly one character
			if si >= sLen {
				return false
			}
			pi++
			si++

		default:
			// Literal character must match
			if si >= sLen || pattern[pi] != str[si] {
				return false
			}
			pi++
			si++
		}
	}

	// Pattern exhausted - match if string also exhausted
	return si == sLen
}
