package transfer

// State is where a Transfer is in its life.
//
//	                ┌── deny ──────────> denied
//	                │
//	pending ─accept──┼── expire (60s) ──> expired
//	   │             │
//	   │             └── attach+splice ─> streaming ──> completed
//	   │                                      │
//	   └── cancel ────────────> canceled      └── error ──> failed
type State string

const (
	StatePending   State = "pending"
	StateAccepted  State = "accepted"
	StateStreaming State = "streaming"
	StateCompleted State = "completed"
	StateDenied    State = "denied"
	StateExpired   State = "expired"
	StateCanceled  State = "canceled"
	StateFailed    State = "failed"
)

// Kind is the shape of a Payload. It decides whether the relay passes bytes
// through or zips them, and it comes from the Sender's declaration rather than
// from inspecting the parts (ADR 0004).
type Kind string

const (
	KindFile   Kind = "file"
	KindFolder Kind = "folder"
)

// Failure reasons, from DESIGN §7.
const (
	ReasonSenderDisconnected    = "sender_disconnected"
	ReasonRecipientDisconnected = "recipient_disconnected"
	ReasonRendezvousTimeout     = "rendezvous_timeout"
	ReasonOfferExpired          = "offer_expired"
	ReasonPayloadMismatch       = "payload_mismatch"
	ReasonInstanceDraining      = "instance_draining"
	ReasonInternal              = "internal"
)

// terminal states accept no further transitions.
var terminal = map[State]bool{
	StateCompleted: true,
	StateDenied:    true,
	StateExpired:   true,
	StateCanceled:  true,
	StateFailed:    true,
}
