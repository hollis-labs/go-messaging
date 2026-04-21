package messaging

// AddressKind categorizes what an Address points at.
type AddressKind string

const (
	KindAgent    AddressKind = "agent"
	KindUser     AddressKind = "user"
	KindService  AddressKind = "service"
	KindSession  AddressKind = "session"
	KindWorkflow AddressKind = "workflow"
)

// Address is the typed in-memory form of a messaging URN.
//
// Canonical wire form: msg://<kind>/<authority>/<id>[/<subid>]
type Address struct {
	Kind      AddressKind
	Authority string
	ID        string
	SubID     string // optional
}

// IsZero reports whether the Address is uninitialized.
func (a Address) IsZero() bool {
	return a.Kind == "" && a.Authority == "" && a.ID == "" && a.SubID == ""
}
