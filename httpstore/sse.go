package httpstore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// maxEventBytes caps one SSE event (its data and event-name fields). An event
// over the cap is dropped, reported, and the stream carries on.
const maxEventBytes = 1 << 20

var errEventTooLarge = fmt.Errorf("httpstore: SSE event larger than %d bytes dropped", maxEventBytes)

// errStopped is returned by readSSE when emit asks to stop.
var errStopped = errors.New("httpstore: stream consumer stopped")

type sseEvent struct {
	Name string // "event" field; "" when absent
	Data string // "data" fields joined by "\n"
}

// readSSE parses a text/event-stream from r and calls emit for each complete
// event named "message" (or unnamed), the only kind the servers send;
// comments, other event names, id and retry are ignored.
//
// It follows the WHATWG event-stream grammar: lines end in LF, CR or CRLF; a
// "data" field's single leading space is optional; several data lines join
// with LF; an event with no data lines is not dispatched; an event still
// open at end of stream is discarded; a leading UTF-8 BOM is ignored. An
// event larger than maxBytes bytes is dropped, reported through onErr, and
// parsing resumes at the next event, so one oversized frame cannot end the
// stream or grow memory without bound.
//
// It returns io.EOF at a clean end of stream, another error if the read
// failed, and errStopped if emit returned false.
func readSSE(r io.Reader, maxBytes int, emit func(sseEvent) bool, onErr func(error)) error {
	// A line may be longer than the event cap by its field name ("event: " is
	// the longest); the size accounting below enforces the cap exactly.
	lr := lineReader{br: bufio.NewReaderSize(r, 32<<10), limit: maxBytes + len("event: ")}
	var (
		buf      []byte
		name     string
		data     []byte
		hasData  bool
		size     int
		overflow bool
		first    = true
	)
	reset := func() {
		name, data, hasData, size, overflow = "", data[:0], false, 0, false
	}
	for {
		line, truncated, err := lr.next(buf)
		buf = line[:0]
		if err != nil {
			return err
		}
		if first {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			first = false
		}
		if truncated {
			overflow = true
		}
		if len(line) == 0 {
			// Blank line: dispatch.
			switch {
			case overflow:
				onErr(errEventTooLarge)
			case hasData:
				ev := sseEvent{Name: name, Data: string(bytes.TrimSuffix(data, []byte("\n")))}
				if ev.Name == "" || ev.Name == "message" {
					if !emit(ev) {
						return errStopped
					}
				}
			}
			reset()
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value := line, []byte(nil)
		if i := bytes.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], line[i+1:]
			value = bytes.TrimPrefix(value, []byte(" "))
		}
		if overflow {
			continue
		}
		switch string(field) {
		case "data":
			size += len(value) + 1
			if size > maxBytes {
				overflow, data = true, data[:0]
				continue
			}
			data = append(append(data, value...), '\n')
			hasData = true
		case "event":
			size += len(value)
			if size > maxBytes {
				overflow, data = true, data[:0]
				continue
			}
			name = string(value)
		}
	}
}

// lineReader splits a stream into lines ending in LF, CR or CRLF without
// ever blocking to look ahead after a CR.
type lineReader struct {
	br        *bufio.Reader
	limit     int
	pendingCR bool // the previous line ended in CR; swallow an immediately following LF
}

// next appends the next line's bytes to buf[:0] and returns them. Bytes past
// limit are discarded and truncated is set. A final line with no terminator
// is dropped: err is then io.EOF (or the read error).
func (l *lineReader) next(buf []byte) (line []byte, truncated bool, err error) {
	buf = buf[:0]
	for {
		c, err := l.br.ReadByte()
		if err != nil {
			return buf, truncated, err
		}
		if l.pendingCR {
			l.pendingCR = false
			if c == '\n' {
				continue
			}
		}
		switch c {
		case '\n':
			return buf, truncated, nil
		case '\r':
			l.pendingCR = true
			return buf, truncated, nil
		default:
			if len(buf) < l.limit {
				buf = append(buf, c)
			} else {
				truncated = true
			}
		}
	}
}
