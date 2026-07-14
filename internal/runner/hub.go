package runner

import (
	"encoding/json"
	"sync"
)

// Event is one progress event from a run, broadcast over SSE.
type Event struct {
	ID   int            `json:"id"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

// Hub keeps a per-run ring buffer of events (Last-Event-ID replay) and fans
// out live events to subscribers. Events are ephemeral by design — the
// durable record is the report's audit appendix.
type Hub struct {
	mu     sync.Mutex
	runs   map[int64]*ring
}

type ring struct {
	events []Event // capped
	nextID int
	subs   map[chan Event]struct{}
	closed bool
}

const ringCap = 2000

func NewHub() *Hub {
	return &Hub{runs: map[int64]*ring{}}
}

func (h *Hub) get(runID int64) *ring {
	if r, ok := h.runs[runID]; ok {
		return r
	}
	r := &ring{subs: map[chan Event]struct{}{}, nextID: 1}
	h.runs[runID] = r
	return r
}

func (h *Hub) Publish(runID int64, e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.get(runID)
	e.ID = r.nextID
	r.nextID++
	r.events = append(r.events, e)
	if len(r.events) > ringCap {
		r.events = r.events[len(r.events)-ringCap:]
	}
	for ch := range r.subs {
		select {
		case ch <- e:
		default: // slow subscriber: drop rather than block the run
		}
	}
	if e.Type == "finished" {
		r.closed = true
		for ch := range r.subs {
			close(ch)
			delete(r.subs, ch)
		}
	}
}

// Subscribe returns buffered replay events (those with ID > lastEventID) and,
// unless the run already finished, a live channel (nil when finished).
func (h *Hub) Subscribe(runID int64, lastEventID int) ([]Event, chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.get(runID)
	var replay []Event
	for _, e := range r.events {
		if e.ID > lastEventID {
			replay = append(replay, e)
		}
	}
	if r.closed {
		return replay, nil
	}
	ch := make(chan Event, 64)
	r.subs[ch] = struct{}{}
	return replay, ch
}

func (h *Hub) Unsubscribe(runID int64, ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.runs[runID]; ok {
		if _, subscribed := r.subs[ch]; subscribed {
			delete(r.subs, ch)
			close(ch)
		}
	}
}

func (e Event) Marshal() []byte {
	b, _ := json.Marshal(e)
	return b
}
