package group

import (
	"sync"
	"time"

	"github.com/sagernet/sing-box/common/urltest"
)

// FlowRecord is one connection worth a look afterwards. Node events say a node stalled;
// these say which stream, how it was opened, how long it waited on the far end and on the
// client, and who ended it. A video that froze for half a minute until the player gave up
// left no trace at all before: the stall counters only saw gaps the stream recovered from.
type FlowRecord struct {
	Seq   int64  `json:"seq"`
	At    int64  `json:"at"` // when the flow ended (unix ms)
	Net   string `json:"net"`
	Group string `json:"group"`
	Site  string `json:"site,omitempty"`
	Node  string `json:"node"`
	// how long the race took to hand over a connection, the first answer, and what the race
	// tried on the way (only when it tried more than one node or took long)
	OpenMs int64  `json:"open_ms,omitempty"`
	TTFBMs int64  `json:"ttfb_ms,omitempty"`
	Tried  string `json:"tried,omitempty"`
	AgeMs  int64  `json:"age_ms"`
	Down   int64  `json:"down,omitempty"`
	Up     int64  `json:"up,omitempty"`
	// the longest silence of the far end in the middle of an answer while a read was pending,
	// and the longest the client took to take the data read before it (backpressure); a flow
	// that ended inside such a silence counts it up to the end
	MidWaitMs int64 `json:"mid_wait_ms,omitempty"`
	ClientMs  int64 `json:"client_ms,omitempty"`
	// the far end was silent in the middle of an answer when the flow ended
	EndedMid bool `json:"ended_mid,omitempty"`
	Stalls   int  `json:"stalls,omitempty"`
	Moves    int  `json:"moves,omitempty"`
	// who ended the flow: client, remote (with the error), or the engine and why
	Close string `json:"close"`
}

const maxFlows = 4000

type flowLog struct {
	access sync.Mutex
	seq    int64
	ring   []FlowRecord
	next   int
	full   bool
}

func (l *flowLog) add(rec FlowRecord) {
	l.access.Lock()
	defer l.access.Unlock()
	if l.ring == nil {
		l.ring = make([]FlowRecord, maxFlows)
	}
	l.seq++
	rec.Seq = l.seq
	rec.At = time.Now().UnixMilli()
	l.ring[l.next] = rec
	l.next = (l.next + 1) % maxFlows
	if l.next == 0 {
		l.full = true
	}
}

// since returns the records after seq, oldest first.
func (l *flowLog) since(seq int64) []FlowRecord {
	records, _ := l.after(seq)
	return records
}

// after is since plus the newest sequence number; it runs from 1 again after a restart, which
// is how a reader holding a larger one can tell.
func (l *flowLog) after(seq int64) ([]FlowRecord, int64) {
	l.access.Lock()
	defer l.access.Unlock()
	count := l.next
	if l.full {
		count = maxFlows
	}
	var result []FlowRecord
	for i := count; i >= 1; i-- {
		rec := l.ring[(l.next-i+maxFlows)%maxFlows]
		if rec.Seq > seq {
			result = append(result, rec)
		}
	}
	return result, l.seq
}

// Flows returns the flow records after seq of the box that owns history, oldest first, and
// the newest sequence number.
func Flows(history *urltest.HistoryStorage, seq int64) ([]FlowRecord, int64) {
	registriesAccess.Lock()
	r := registries[history]
	registriesAccess.Unlock()
	if r == nil {
		return nil, 0
	}
	return r.flows.after(seq)
}

// A flow is kept when something about it was off, and big downloads always, as the baseline
// the anomalies are read against.
const (
	flowSlowOpen = time.Second
	flowMidWait  = 2 * time.Second
	flowBulk     = 1 << 20
)

// flowNotable reports whether a finished TCP flow is worth a record.
func flowNotable(rec *FlowRecord) bool {
	switch {
	case rec.OpenMs >= flowSlowOpen.Milliseconds(), rec.Tried != "":
		return true
	case rec.MidWaitMs >= flowMidWait.Milliseconds(), rec.EndedMid, rec.Stalls > 0:
		return true
	case rec.Close != "client" && rec.Close != "remote: EOF":
		return true
	}
	return rec.Down >= flowBulk
}
