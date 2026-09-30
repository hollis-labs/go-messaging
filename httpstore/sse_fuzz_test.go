package httpstore

import (
	"bufio"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func bufioReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }

// FuzzReadSSE feeds arbitrary bytes to the reader and checks the properties
// that must hold for any input: it terminates without panicking, no event
// exceeds the cap, and the result does not depend on how the stream is
// chunked.
func FuzzReadSSE(f *testing.F) {
	for _, seed := range []string{
		"",
		"data: x\n\n",
		"event: message\r\ndata: {\"id\":\"1\"}\r\n\r\n",
		"data: a\rdata: b\r\r",
		": ping\n\ndata:\n\n",
		"\xef\xbb\xbfdata: x\n\n",
		"data: " + strings.Repeat("a", 200) + "\n\ndata: ok\n\n",
		"event: message\ndata: 1\ndata: 2\n\nid: 9\n\n",
		"\r\n\r\n\n\r",
	} {
		f.Add([]byte(seed))
	}
	const limit = 128
	f.Fuzz(func(t *testing.T, in []byte) {
		run := func(wrap func(io.Reader) io.Reader) ([]sseEvent, int) {
			var evs []sseEvent
			var errs int
			src := wrap(strings.NewReader(string(in)))
			_ = readSSE(src, limit, func(e sseEvent) bool {
				evs = append(evs, e)
				return true
			}, func(error) { errs++ })
			return evs, errs
		}
		whole, wholeErrs := run(func(r io.Reader) io.Reader { return r })
		bytewise, byteErrs := run(iotest.OneByteReader)
		half, halfErrs := run(iotest.HalfReader)
		if !reflect.DeepEqual(whole, bytewise) || wholeErrs != byteErrs {
			t.Fatalf("chunking changed the result: whole=%q/%d bytewise=%q/%d", whole, wholeErrs, bytewise, byteErrs)
		}
		if !reflect.DeepEqual(whole, half) || wholeErrs != halfErrs {
			t.Fatalf("chunking changed the result: whole=%q/%d half=%q/%d", whole, wholeErrs, half, halfErrs)
		}
		for _, e := range whole {
			if len(e.Data) > limit {
				t.Fatalf("event of %d bytes exceeds the %d cap", len(e.Data), limit)
			}
			if e.Name != "" && e.Name != "message" {
				t.Fatalf("event named %q was dispatched", e.Name)
			}
		}
	})
}
