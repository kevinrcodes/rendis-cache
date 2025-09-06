package resp_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"my-redis/internal/resp"
)

func TestValueEncode(t *testing.T) {
	tests := []struct {
		name  string
		value resp.Value
		want  string
	}{
		{"simple string", resp.SimpleString("OK"), "+OK\r\n"},
		{"error", resp.Error("ERR nope"), "-ERR nope\r\n"},
		{"errorf", resp.Errorf("ERR unknown command '%s'", "foo"), "-ERR unknown command 'foo'\r\n"},
		{"integer", resp.Integer(42), ":42\r\n"},
		{"negative integer", resp.Integer(-1), ":-1\r\n"},
		{"bulk string", resp.BulkString("hey"), "$3\r\nhey\r\n"},
		{"empty bulk string", resp.BulkString(""), "$0\r\n\r\n"},
		{"binary safe bulk string", resp.BulkString("a\r\nb"), "$4\r\na\r\nb\r\n"},
		{"nil", resp.Nil(), "$-1\r\n"},
		{"empty array", resp.Array(), "*0\r\n"},
		{
			"array of bulk strings",
			resp.Array(resp.BulkString("ECHO"), resp.BulkString("hey")),
			"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
		},
		{
			"nested array",
			resp.Array(resp.Integer(1), resp.Array(resp.SimpleString("a"))),
			"*2\r\n:1\r\n*1\r\n+a\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(tt.value.Encode()); got != tt.want {
				t.Errorf("Encode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			"ping",
			"*1\r\n$4\r\nPING\r\n",
			[][]string{{"PING"}},
		},
		{
			"echo with argument",
			"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
			[][]string{{"ECHO", "hey"}},
		},
		{
			"several commands in one stream",
			"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n",
			[][]string{{"PING"}, {"PING"}},
		},
		{
			"argument containing CRLF",
			"*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n",
			[][]string{{"SET", "a\r\nb"}},
		},
		{
			"empty argument",
			"*2\r\n$3\r\nGET\r\n$0\r\n\r\n",
			[][]string{{"GET", ""}},
		},
		{
			"empty array is skipped",
			"*0\r\n*1\r\n$4\r\nPING\r\n",
			[][]string{nil, {"PING"}},
		},
		{
			"inline command",
			"PING\r\n",
			[][]string{{"PING"}},
		},
		{
			"inline command with arguments and extra spaces",
			"SET  foo   bar\r\n",
			[][]string{{"SET", "foo", "bar"}},
		},
		{
			"empty inline command",
			"\r\n",
			[][]string{nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := resp.NewReader(strings.NewReader(tt.input))
			for i, want := range tt.want {
				got, err := r.ReadCommand()
				if err != nil {
					t.Fatalf("command %d: unexpected error: %v", i, err)
				}
				if !equal(got, want) {
					t.Fatalf("command %d = %q, want %q", i, got, want)
				}
			}
			if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
				t.Errorf("after last command: error = %v, want io.EOF", err)
			}
		})
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"array length not a number", "*x\r\n"},
		{"array length too large", "*99999999\r\n"},
		{"negative array length beyond null", "*-2\r\n"},
		{"element is not a bulk string", "*1\r\n+PING\r\n"},
		{"null bulk string argument", "*1\r\n$-1\r\n"},
		{"bulk length longer than payload", "*1\r\n$10\r\nPING\r\n"},
		{"bulk string not CRLF terminated", "*1\r\n$4\r\nPINGxx"},
		{"line terminated by bare LF", "*1\n"},
		{"truncated header", "*1\r\n$4\r\n"},
		{"line longer than the read buffer", "*1\r\n$4\r\n" + strings.Repeat("x", 64*1024)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := resp.NewReader(strings.NewReader(tt.input))
			_, err := r.ReadCommand()
			if !errors.Is(err, resp.ErrProtocol) {
				t.Errorf("error = %v, want one wrapping ErrProtocol", err)
			}
		})
	}
}

func TestReadCommandEOFOnEmptyStream(t *testing.T) {
	r := resp.NewReader(strings.NewReader(""))
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Errorf("error = %v, want io.EOF", err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
