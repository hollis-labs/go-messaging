package httpstore

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

// parse runs readSSE over in with the given per-event cap and returns the
// dispatched events, the reported errors and the terminal error.
func parse(in string, max int, r func(io.Reader) io.Reader) (events []sseEvent, errs []error, end error) {
	var src io.Reader = strings.NewReader(in)
	if r != nil {
		src = r(src)
	}
	end = readSSE(src, max, func(e sseEvent) bool {
		events = append(events, e)
		return true
	}, func(err error) { errs = append(errs, err) })
	return
}

func ev(data string) sseEvent { return sseEvent{Data: data} }

func TestReadSSE_Vectors(t *testing.T) {
	const big = maxEventBytes
	cases := []struct {
		name string
		in   string
		want []sseEvent
	}{
		{"single LF", "data: x\n\n", []sseEvent{ev("x")}},
		{"named message event (what Tether sends)", "event: message\ndata: {\"a\":1}\n\n", []sseEvent{{Name: "message", Data: `{"a":1}`}}},
		{"data without a space", "data:x\n\n", []sseEvent{ev("x")}},
		{"only one leading space is stripped", "data:  x\n\n", []sseEvent{ev(" x")}},
		{"colon inside the value", "data: a:b:c\n\n", []sseEvent{ev("a:b:c")}},
		{"multi-line data joins with LF", "data: a\ndata: b\ndata:\ndata: d\n\n", []sseEvent{ev("a\nb\n\nd")}},
		{"empty data line dispatches an empty event", "data\n\n", []sseEvent{ev("")}},
		{"CRLF", "data: x\r\n\r\ndata: y\r\n\r\n", []sseEvent{ev("x"), ev("y")}},
		{"lone CR", "data: x\r\rdata: y\r\r", []sseEvent{ev("x"), ev("y")}},
		{"mixed line ends", "data: a\r\ndata: b\ndata: c\r\r", []sseEvent{ev("a\nb\nc")}},
		{"comment and ping lines are ignored", ": ping\n\ndata: x\n: mid\n\n", []sseEvent{ev("x")}},
		{"a comment-only block dispatches nothing", ": hello\n\n", nil},
		{"event without data is not dispatched", "event: message\n\n", nil},
		{"id and retry are ignored", "id: 7\nretry: 100\ndata: x\n\n", []sseEvent{ev("x")}},
		{"unknown field ignored", "foo: bar\ndata: x\n\n", []sseEvent{ev("x")}},
		{"other event names are filtered out", "event: error\ndata: {}\n\nevent: message\ndata: ok\n\n", []sseEvent{{Name: "message", Data: "ok"}}},
		{"blank lines between events", "\n\ndata: a\n\n\n\ndata: b\n\n", []sseEvent{ev("a"), ev("b")}},
		{"event still open at end of stream is discarded", "data: a\n\ndata: b\n", []sseEvent{ev("a")}},
		{"final data line without newline is discarded", "data: a\n\ndata: b", []sseEvent{ev("a")}},
		{"leading BOM", "\xef\xbb\xbfdata: x\n\n", []sseEvent{ev("x")}},
		{"BOM only counts at the very start", "data: a\n\n\xef\xbb\xbfdata: b\n\n", []sseEvent{ev("a")}},
		{"event name resets between events", "event: other\ndata: a\n\ndata: b\n\n", []sseEvent{ev("b")}},
		{"unicode passes through", "data: héllo ☃\n\n", []sseEvent{ev("héllo ☃")}},
		{"event exactly at the cap is delivered", "data: " + strings.Repeat("a", big-1) + "\n\n", []sseEvent{ev(strings.Repeat("a", big-1))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, errs, end := parse(tc.in, big, nil)
			if !errors.Is(end, io.EOF) {
				t.Errorf("end = %v, want io.EOF", end)
			}
			if len(errs) != 0 {
				t.Errorf("unexpected errors: %v", errs)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("events = %q, want %q", got, tc.want)
			}
			// Splitting the stream at every byte must not change anything.
			got1, errs1, _ := parse(tc.in, big, iotest.OneByteReader)
			if !reflect.DeepEqual(got1, tc.want) || len(errs1) != 0 {
				t.Errorf("one-byte reads: events = %q errs = %v, want %q", got1, errs1, tc.want)
			}
		})
	}
}

func TestReadSSE_OversizedEventIsDroppedAndStreamContinues(t *testing.T) {
	const limit = 64
	in := "data: first\n\n" +
		"data: " + strings.Repeat("x", 100) + "\n\n" + // one huge line
		"data: " + strings.Repeat("y", 40) + "\ndata: " + strings.Repeat("z", 40) + "\n\n" + // many lines summing over the cap
		"data: last\n\n"
	got, errs, end := parse(in, limit, nil)
	if !errors.Is(end, io.EOF) {
		t.Fatalf("end = %v", end)
	}
	if want := []sseEvent{ev("first"), ev("last")}; !reflect.DeepEqual(got, want) {
		t.Errorf("events = %q, want %q", got, want)
	}
	if len(errs) != 2 {
		t.Fatalf("errors = %v, want one per oversized event", errs)
	}
	for _, err := range errs {
		if !errors.Is(err, errEventTooLarge) {
			t.Errorf("error %v, want errEventTooLarge", err)
		}
	}
}

func TestReadSSE_HugeLineWithoutNewlineIsBounded(t *testing.T) {
	// A hostile 8 MiB line must not be buffered in full. lineReader stops
	// storing at the limit; assert through its state.
	lr := lineReader{br: bufioReader(strings.NewReader(strings.Repeat("a", 8<<20) + "\n")), limit: 1024}
	line, truncated, err := lr.next(nil)
	if err != nil || !truncated || len(line) != 1024 {
		t.Fatalf("len=%d truncated=%v err=%v, want 1024 bytes, truncated", len(line), truncated, err)
	}
}

func TestReadSSE_StopRequestedByConsumer(t *testing.T) {
	n := 0
	err := readSSE(strings.NewReader("data: a\n\ndata: b\n\ndata: c\n\n"), maxEventBytes,
		func(sseEvent) bool { n++; return n < 2 }, func(error) {})
	if !errors.Is(err, errStopped) || n != 2 {
		t.Errorf("err=%v after %d events, want errStopped after 2", err, n)
	}
}

func TestReadSSE_ReadErrorIsReturned(t *testing.T) {
	boom := errors.New("boom")
	got, _, end := parse("data: a\n\ndata: b", maxEventBytes, func(r io.Reader) io.Reader {
		return io.MultiReader(r, iotest.ErrReader(boom))
	})
	if !errors.Is(end, boom) {
		t.Errorf("end = %v, want boom", end)
	}
	if want := []sseEvent{ev("a")}; !reflect.DeepEqual(got, want) {
		t.Errorf("events = %q, want %q", got, want)
	}
}
