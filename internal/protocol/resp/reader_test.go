package resp

import (
	"bytes"
	"testing"
)

func TestTokenizeInline(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [][]byte
		wantErr bool
	}{
		// Basic commands
		{
			name:  "simple command",
			input: "PING",
			want:  [][]byte{[]byte("PING")},
		},
		{
			name:  "command with args",
			input: "SET key value",
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "multiple spaces between args",
			input: "SET   key    value",
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "tabs as separators",
			input: "SET\tkey\tvalue",
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "mixed spaces and tabs",
			input: "SET \t key \t value",
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},

		// Double-quoted strings
		{
			name:  "double quoted value with spaces",
			input: `SET key "hello world"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello world")},
		},
		{
			name:  "double quoted value with multiple spaces",
			input: `SET key "hello   world"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello   world")},
		},
		{
			name:  "double quoted key and value",
			input: `SET "my key" "my value"`,
			want:  [][]byte{[]byte("SET"), []byte("my key"), []byte("my value")},
		},
		{
			name:  "empty double quoted string",
			input: `SET key ""`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("")},
		},
		{
			name:  "double quoted with escaped quote",
			input: `SET key "hello \"world\""`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte(`hello "world"`)},
		},
		{
			name:  "double quoted with escaped backslash",
			input: `SET key "path\\to\\file"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte(`path\to\file`)},
		},
		{
			name:  "double quoted with newline escape",
			input: `SET key "line1\nline2"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("line1\nline2")},
		},
		{
			name:  "double quoted with tab escape",
			input: `SET key "col1\tcol2"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("col1\tcol2")},
		},
		{
			name:  "double quoted with carriage return escape",
			input: `SET key "line1\rline2"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("line1\rline2")},
		},
		{
			name:  "double quoted with hex escape",
			input: `SET key "hello\x20world"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello world")},
		},

		// Single-quoted strings
		{
			name:  "single quoted value with spaces",
			input: `SET key 'hello world'`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello world")},
		},
		{
			name:  "single quoted preserves backslash",
			input: `SET key 'hello\nworld'`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte(`hello\nworld`)},
		},
		{
			name:  "empty single quoted string",
			input: `SET key ''`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("")},
		},

		// Mixed quotes
		{
			name:  "mixed single and double quotes",
			input: `SET "my key" 'my value'`,
			want:  [][]byte{[]byte("SET"), []byte("my key"), []byte("my value")},
		},
		{
			name:  "double quote inside single quote",
			input: `SET key 'say "hello"'`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte(`say "hello"`)},
		},
		{
			name:  "single quote inside double quote",
			input: `SET key "it's working"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("it's working")},
		},

		// Real-world examples
		{
			name:  "user message example",
			input: `SET key "hello i am shivang"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello i am shivang")},
		},
		{
			name:  "JSON value",
			input: `SET user:1 '{"name":"John","age":30}'`,
			want:  [][]byte{[]byte("SET"), []byte("user:1"), []byte(`{"name":"John","age":30}`)},
		},
		{
			name:  "URL value",
			input: `SET url "https://example.com/path?query=value&foo=bar"`,
			want:  [][]byte{[]byte("SET"), []byte("url"), []byte("https://example.com/path?query=value&foo=bar")},
		},
		{
			name:  "SET with EX option",
			input: `SET key "hello world" EX 60`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello world"), []byte("EX"), []byte("60")},
		},
		{
			name:  "MSET with multiple values",
			input: `MSET key1 "value 1" key2 "value 2"`,
			want:  [][]byte{[]byte("MSET"), []byte("key1"), []byte("value 1"), []byte("key2"), []byte("value 2")},
		},

		// Edge cases
		{
			name:  "quoted string at start",
			input: `"SET" key value`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "only quoted string",
			input: `"PING"`,
			want:  [][]byte{[]byte("PING")},
		},
		{
			name:  "adjacent quoted strings",
			input: `SET "key""value"`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "leading whitespace",
			input: `   SET key value`,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},
		{
			name:  "trailing whitespace",
			input: `SET key value   `,
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("value")},
		},

		// Error cases
		{
			name:    "unclosed double quote",
			input:   `SET key "hello world`,
			wantErr: true,
		},
		{
			name:    "unclosed single quote",
			input:   `SET key 'hello world`,
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "only whitespace",
			input:   "   ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tokenizeInline([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("tokenizeInline() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !bytesSlicesEqual(got, tt.want) {
				t.Errorf("tokenizeInline() = %q, want %q", got, tt.want)
			}
		})
	}
}

// bytesSlicesEqual compares two slices of byte slices, treating nil and empty as equal.
func bytesSlicesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestReadInlineIntegration(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [][]byte
		wantErr bool
	}{
		{
			name:  "simple SET with quoted value",
			input: "SET key \"hello world\"\r\n",
			want:  [][]byte{[]byte("SET"), []byte("key"), []byte("hello world")},
		},
		{
			name:  "GET command",
			input: "GET key\r\n",
			want:  [][]byte{[]byte("GET"), []byte("key")},
		},
		{
			name:  "PING",
			input: "PING\r\n",
			want:  [][]byte{[]byte("PING")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(bytes.NewReader([]byte(tt.input)))
			got, err := reader.ReadCommand()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !bytesSlicesEqual(got, tt.want) {
				t.Errorf("ReadCommand() = %q, want %q", got, tt.want)
			}
		})
	}
}
