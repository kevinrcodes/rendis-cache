package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol wraps every malformed-input error returned by a Reader. A server
// should report it to the client and close the connection, since a stream that
// has gone out of sync cannot be resynchronised.
var ErrProtocol = errors.New("protocol error")

// Limits mirroring real Redis, so that a hostile client cannot make the server
// allocate without bound.
const (
	maxBulkLength  = 512 * 1024 * 1024 // 512 MB, the largest Redis string
	maxArrayLength = 1024 * 1024       // elements in a single command

	// readBufferSize bounds how long a single protocol line may be: ReadSlice
	// fails once a line fills the buffer. Bulk payloads are read past the
	// buffer with io.ReadFull, so only headers and inline commands are capped.
	readBufferSize = 16 * 1024
)

// Reader decodes commands from a client connection.
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a Reader that decodes commands from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readBufferSize)}
}

// ReadCommand reads the next command and returns its arguments, the first of
// which is the command name. It understands both the array-of-bulk-strings form
// that real clients send and the inline form that a telnet or netcat session
// produces.
//
// It returns io.EOF when the client has closed the connection cleanly, and an
// error wrapping ErrProtocol when the input is malformed. A command with no
// arguments (an empty inline line, or "*0\r\n") yields a nil slice and no error;
// callers should skip it.
func (r *Reader) ReadCommand() ([]string, error) {
	prefix, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if Type(prefix[0]) != TypeArray {
		return r.readInlineCommand()
	}
	return r.readArrayCommand()
}

func (r *Reader) readArrayCommand() ([]string, error) {
	n, err := r.readPrefixedLength(TypeArray, maxArrayLength)
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	args := make([]string, n)
	for i := range args {
		if args[i], err = r.readBulkString(); err != nil {
			return nil, err
		}
	}
	return args, nil
}

// readBulkString reads one "$<len>\r\n<bytes>\r\n" element.
func (r *Reader) readBulkString() (string, error) {
	n, err := r.readPrefixedLength(TypeBulkString, maxBulkLength)
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", fmt.Errorf("%w: unexpected null bulk string in command", ErrProtocol)
	}
	buf := make([]byte, n+2) // payload plus its CRLF terminator
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return "", eofAsUnexpected(err)
	}
	if string(buf[n:]) != crlf {
		return "", fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
	}
	return string(buf[:n]), nil
}

// readPrefixedLength reads a "<want><number>\r\n" header and returns the number,
// which may be -1 to denote a null bulk string.
func (r *Reader) readPrefixedLength(want Type, max int) (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	if len(line) == 0 || Type(line[0]) != want {
		return 0, fmt.Errorf("%w: expected %q, got %q", ErrProtocol, string(want), line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return 0, fmt.Errorf("%w: invalid length %q", ErrProtocol, line[1:])
	}
	if n < -1 || n > max {
		return 0, fmt.Errorf("%w: length %d out of range", ErrProtocol, n)
	}
	return n, nil
}

// readInlineCommand reads a whitespace-separated command line, the format a
// human typing into netcat produces.
func (r *Reader) readInlineCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(line)), nil
}

// readLine reads one CRLF-terminated line and returns it without the terminator.
// The returned bytes are only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		return nil, fmt.Errorf("%w: line exceeds %d bytes", ErrProtocol, readBufferSize)
	case err != nil:
		return nil, eofAsUnexpected(err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: line not terminated by CRLF", ErrProtocol)
	}
	return line[:len(line)-2], nil
}

// eofAsUnexpected converts an EOF found part way through a command into a
// protocol error, so that only a clean command boundary reports io.EOF.
func eofAsUnexpected(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: unexpected end of input", ErrProtocol)
	}
	return err
}
