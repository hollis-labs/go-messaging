package httpstore_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
)

// TestErrorTable is the package's one status-to-error table, exercised over
// both error body shapes and on both profiles.
func TestErrorTable(t *testing.T) {
	flat := func(msg string) string { return `{"error":"` + msg + `"}` }
	nested := func(code, msg string) string { return `{"error":{"code":"` + code + `","message":"` + msg + `"}}` }

	cases := []struct {
		name      string
		status    int
		body      string
		is        []error
		notIs     []error
		wantCode  string
		wantMsg   string
		wantExact bool // must be a bare *StatusError, no sentinel
	}{
		{"404 flat", 404, flat("gone"), []error{messaging.ErrNotFound}, nil, "", "gone", false},
		{"404 nested", 404, nested("not_found", "message not found"), []error{messaging.ErrNotFound}, nil, "not_found", "message not found", false},
		{"409", 409, nested("conflict", "not the recipient"), []error{httpstore.ErrWrongRecipient}, []error{messaging.ErrNotFound}, "conflict", "not the recipient", false},
		{"422 flat", 422, flat("bad"), []error{messaging.ErrPresetLifecycle}, nil, "", "bad", false},
		{"preset_lifecycle code on 400", 400, nested("preset_lifecycle", "no"), []error{messaging.ErrPresetLifecycle}, nil, "preset_lifecycle", "no", false},
		{"401", 401, flat("who are you"), []error{messaging.ErrStoreUnavailable}, []error{messaging.ErrNotFound}, "", "who are you", false},
		{"403 nested", 403, nested("forbidden", "as must match to"), []error{messaging.ErrStoreUnavailable}, []error{messaging.ErrNotFound}, "forbidden", "as must match to", false},
		{"503", 503, flat("down"), []error{messaging.ErrStoreUnavailable}, nil, "", "down", false},
		{"400 is a StatusError", 400, nested("invalid_request", "as is required"), nil,
			[]error{messaging.ErrNotFound, messaging.ErrStoreUnavailable, messaging.ErrPresetLifecycle}, "invalid_request", "as is required", true},
		{"500 is a StatusError", 500, flat("oops"), nil, []error{messaging.ErrStoreUnavailable}, "", "oops", true},
		{"504 outside Request is a StatusError", 504, "", nil, []error{messaging.ErrRequestTimeout}, "", "", true},
		{"plain text body", 502, "bad gateway\n", nil, nil, "", "bad gateway", true},
		{"html body", 500, "<html>boom</html>", nil, nil, "", "<html>boom</html>", true},
		{"empty body", 418, "", nil, nil, "", "", true},
	}
	for _, p := range []httpstore.Profile{httpstore.TetherProfile(), httpstore.TorqueFederationProfile()} {
		for _, tc := range cases {
			t.Run(p.Name+"/"+tc.name, func(t *testing.T) {
				srv, _ := stub(t, tc.status, "application/json", tc.body)
				s, err := httpstore.New(srv.URL, httpstore.WithProfile(p), httpstore.WithIdentity(alice))
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.Get(context.Background(), "id")
				if err == nil {
					t.Fatal("want an error")
				}
				for _, want := range tc.is {
					if !errors.Is(err, want) {
						t.Errorf("errors.Is(%v, %v) = false", err, want)
					}
				}
				for _, not := range tc.notIs {
					if errors.Is(err, not) {
						t.Errorf("errors.Is(%v, %v) = true", err, not)
					}
				}
				var se *httpstore.StatusError
				if !errors.As(err, &se) {
					t.Fatalf("errors.As(*StatusError) failed for %v", err)
				}
				if se.Code != tc.status || se.ErrCode != tc.wantCode || se.Message != tc.wantMsg {
					t.Errorf("StatusError = %+v, want code %d/%q/%q", se, tc.status, tc.wantCode, tc.wantMsg)
				}
				if tc.wantExact {
					if _, ok := err.(*httpstore.StatusError); !ok {
						t.Errorf("err is %T, want a bare *StatusError", err)
					}
				}
			})
		}
	}
}

// Any 2xx is success (Tether's peer store did this; Torque's 201/204 both fit).
func TestAnySuccessStatusIsAccepted(t *testing.T) {
	for _, status := range []int{200, 201, 202, 204} {
		srv, _ := stub(t, status, "application/json", "")
		s, err := httpstore.New(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(context.Background(), "id", bob); err != nil {
			t.Errorf("Consume with %d: %v", status, err)
		}
		if err := s.Cancel(context.Background(), "id"); err != nil {
			t.Errorf("Cancel with %d: %v", status, err)
		}
	}
	for _, status := range []int{200, 201, 202} {
		srv, _ := stub(t, status, "application/json", `{"id":"1","kind":"notice"}`)
		s, err := httpstore.New(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Send(context.Background(), notice(alice, bob))
		if err != nil || got.ID != "1" {
			t.Errorf("Send with %d: %+v, %v", status, got, err)
		}
	}
}

func TestUndecodableSuccessBodyIsAnError(t *testing.T) {
	srv, _ := stub(t, http.StatusOK, "application/json", "<<not json>>")
	s, err := httpstore.New(srv.URL, httpstore.WithIdentity(alice))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), "id"); err == nil {
		t.Fatal("a 200 with garbage must not look like success")
	}
	if _, err := s.Inbox(context.Background(), bob, messaging.Filter{}); err == nil {
		t.Fatal("a 200 with garbage must not look like an empty inbox")
	}
}
