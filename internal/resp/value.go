// Package resp implements the subset of the Redis Serialization Protocol
// (RESP2) that a key-value server needs: decoding commands sent by clients and
// encoding the replies sent back.
//
// See https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"fmt"
	"io"
	"strconv"
)

// Type is the leading byte that identifies a RESP value on the wire.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

const crlf = "\r\n"

// Value is a single RESP value. The zero Value is not meaningful; build values
// with the constructors below so that only encodable states are representable.
type Value struct {
	typ   Type
	str   string
	num   int64
	array []Value
	null  bool
}

// SimpleString returns a RESP simple string, used for short status replies such
// as "+OK". It must not contain CR or LF.
func SimpleString(s string) Value { return Value{typ: TypeSimpleString, str: s} }

// Error returns a RESP error reply. By convention the message starts with an
// uppercase error code, e.g. "ERR unknown command".
func Error(msg string) Value { return Value{typ: TypeError, str: msg} }

// Errorf returns a RESP error reply built from a format string.
func Errorf(format string, args ...any) Value {
	return Error(fmt.Sprintf(format, args...))
}

// Integer returns a RESP integer reply.
func Integer(n int64) Value { return Value{typ: TypeInteger, num: n} }

// BulkString returns a RESP bulk string, the type used for arbitrary binary
// safe payloads such as stored values.
func BulkString(s string) Value { return Value{typ: TypeBulkString, str: s} }

// Nil returns the RESP null bulk string, the reply for a missing key.
func Nil() Value { return Value{typ: TypeBulkString, null: true} }

// Array returns a RESP array of the given values.
func Array(values ...Value) Value { return Value{typ: TypeArray, array: values} }

// Append encodes v and appends the bytes to dst, returning the extended slice.
func (v Value) Append(dst []byte) []byte {
	switch v.typ {
	case TypeSimpleString, TypeError:
		dst = append(dst, byte(v.typ))
		dst = append(dst, v.str...)
	case TypeInteger:
		dst = append(dst, byte(v.typ))
		dst = strconv.AppendInt(dst, v.num, 10)
	case TypeBulkString:
		dst = append(dst, byte(v.typ))
		if v.null {
			dst = append(dst, "-1"...)
			break
		}
		dst = strconv.AppendInt(dst, int64(len(v.str)), 10)
		dst = append(dst, crlf...)
		dst = append(dst, v.str...)
	case TypeArray:
		dst = append(dst, byte(v.typ))
		dst = strconv.AppendInt(dst, int64(len(v.array)), 10)
		dst = append(dst, crlf...)
		for _, elem := range v.array {
			dst = elem.Append(dst)
		}
		// Each element already terminated itself.
		return dst
	default:
		panic(fmt.Sprintf("resp: cannot encode value of type %q", v.typ))
	}
	return append(dst, crlf...)
}

// Encode returns the wire representation of v.
func (v Value) Encode() []byte { return v.Append(nil) }

// WriteTo writes the wire representation of v to w.
func (v Value) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(v.Encode())
	return int64(n), err
}
