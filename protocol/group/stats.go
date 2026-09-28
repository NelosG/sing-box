package group

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// nodeStats are counters fed by real traffic. Beyond the panel, they are what the engine
// learns from: time to first byte of real connections is a far denser latency signal than
// any probe, and bytes per second on real downloads show what a node can actually carry.
type nodeStats struct {
	bytesDown   atomic.Int64
	bytesUp     atomic.Int64
	raceWins    atomic.Int64
	raceLosses  atomic.Int64
	dialFails   atomic.Int64
	probeOK     atomic.Int64
	probeFail   atomic.Int64
	deaths      atomic.Int64
	ttfb        atomic.Int64 // EWMA of time to first byte, microseconds
	lastDown    int64        // bytesDown at the previous speed tick
	peakBps     int64        // best 1s download rate within speedWindow
	peakAt      time.Time
	speedSample int64 // last active speed test, bytes per second
	speedAt     time.Time
	// the last speed test that actually finished; speedSample is also stamped before a test
	// starts, with whatever was known, so that a failing test is not retried at once
	testedBps int64
	testedAt  time.Time
	real      realTraffic
	// real-traffic stalls, see nodeConn.noteGap
	stalls          atomic.Int64
	udpStalls       atomic.Int64
	stallMs         atomic.Int64
	lastStallEvent  atomic.Int64
	lastFrozenEvent atomic.Int64
	// mass-silence watch, see speedLoop; touched only by the speed loop
	activeSecs int
	silentSecs int
}

// at most one stall event per node this often; the counters take every stall
const stallEventGap = 2 * time.Second

const (
	silenceActiveBps = 32 << 10 // bytes per second that count as pouring data
	silenceTrigger   = 2        // seconds of pouring before, and of silence after
)

// silenceStep advances the per-second watch with this second's download bytes and reports
// when a node that was pouring data has been completely quiet, with connections open, for
// silenceTrigger seconds.
func silenceStep(s *nodeStats, rate int64, open bool) bool {
	switch {
	case rate >= silenceActiveBps:
		s.activeSecs, s.silentSecs = min(s.activeSecs+1, 60), 0
	case rate == 0 && open:
		s.silentSecs++
		fire := s.silentSecs == silenceTrigger && s.activeSecs >= silenceTrigger
		if s.silentSecs >= silenceTrigger {
			s.activeSecs = 0
		}
		return fire
	default:
		s.activeSecs, s.silentSecs = 0, 0
	}
	return false
}

// takeUrgent reports once that tag wants checking at once, and the event to log for it
// (empty when whoever asked has logged it already).
func (r *healthRegistry) takeUrgent(tag string) (string, bool) {
	r.access.Lock()
	defer r.access.Unlock()
	why, ok := r.urgent[tag]
	if ok {
		delete(r.urgent, tag)
	}
	return why, ok
}

// recordStall counts a stall of real traffic on the node and logs it with its site.
func (r *healthRegistry) recordStall(tag, group, site string, gap time.Duration, udp bool) {
	s := r.stats(tag)
	kind := "stall"
	if udp {
		kind = "udp_stall"
		s.udpStalls.Add(1)
	} else {
		s.stalls.Add(1)
	}
	s.stallMs.Add(gap.Milliseconds())
	now := time.Now().UnixNano()
	last := s.lastStallEvent.Load()
	if now-last < int64(stallEventGap) || !s.lastStallEvent.CompareAndSwap(last, now) {
		return
	}
	r.event(kind, tag, group, fmt.Sprintf("%s %.1fs", site, gap.Seconds()))
}

const speedWindow = 15 * time.Minute

// a TCP stream on a degraded node that received nothing for this long is closed
const degradedStall = 5 * time.Second

type healthEvent struct {
	At     int64  `json:"at"`
	Kind   string `json:"kind"`
	Node   string `json:"node,omitempty"`
	Group  string `json:"group,omitempty"`
	Detail string `json:"detail,omitempty"`
}

const maxEvents = 500

type eventLog struct {
	access sync.Mutex
	events []healthEvent
	next   int
	full   bool
}

func (l *eventLog) add(e healthEvent) {
	e.At = time.Now().UnixMilli()
	l.access.Lock()
	defer l.access.Unlock()
	if l.events == nil {
		l.events = make([]healthEvent, maxEvents)
	}
	l.events[l.next] = e
	l.next = (l.next + 1) % maxEvents
	if l.next == 0 {
		l.full = true
	}
}

// list returns events newest first.
func (l *eventLog) list() []healthEvent {
	l.access.Lock()
	defer l.access.Unlock()
	count := l.next
	if l.full {
		count = maxEvents
	}
	result := make([]healthEvent, 0, count)
	for i := 1; i <= count; i++ {
		result = append(result, l.events[(l.next-i+maxEvents)%maxEvents])
	}
	return result
}

func (r *healthRegistry) stats(tag string) *nodeStats {
	r.access.Lock()
	defer r.access.Unlock()
	return r.statsLocked(tag)
}

func (r *healthRegistry) statsLocked(tag string) *nodeStats {
	s := r.nodeStats[tag]
	if s == nil {
		s = &nodeStats{}
		r.nodeStats[tag] = s
	}
	return s
}

func (r *healthRegistry) event(kind, node, group, detail string) {
	r.events.add(healthEvent{Kind: kind, Node: node, Group: group, Detail: detail})
}

// observeTTFB folds one real connection's time to first byte into the node's average.
func (r *healthRegistry) observeTTFB(tag string, d time.Duration) {
	s := r.stats(tag)
	us := d.Microseconds()
	for {
		old := s.ttfb.Load()
		next := us
		if old != 0 {
			next = old*3/4 + us/4
		}
		if s.ttfb.CompareAndSwap(old, next) {
			return
		}
	}
}

// speedLoop runs tick once a second.
func (r *healthRegistry) speedLoop(done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		r.tick()
	}
}

// tick turns byte counters into a per-node peak download rate and deals with the flows and
// streams that stopped: stalled UDP flows, streams waiting on bad nodes, frozen streams.
func (r *healthRegistry) tick() {
	now := time.Now()
	r.access.Lock()
	var moves []udpMove
	var stuck []*nodeConn
	var frozen []frozenConn
	for tag, n := range r.nodes {
		if n.state == stateDead {
			continue
		}
		// Any failed probe among the last scheduled ones makes the node questionable: a node
		// losing packets fails some probes without turning suspect or lossy (probes wait 3s
		// and mostly get through), and a UDP flow on it stood 25s on the stand.
		bad := n.state == stateSuspect || degradedLocked(n) || n.probeLog != 0
		// A game or call sends packets all the time, so a flow the app keeps sending on that
		// has heard nothing for reapIdle is broken. On a questionable node it moves to
		// another one. On a node that looks fine a QUIC flow is renewed where the group
		// points now, usually the same node: a relay session the node dropped, or a path that
		// died under it, left QUIC apps (Instagram) waiting out their own timeout of half a
		// minute. Other flows stay: a call whose other side is silent looks the same, and a
		// new port or address mid-call drops it.
		for p := range n.packets {
			if p.stalled(reapIdle) && (bad || p.destination.Port == 443) {
				moves = append(moves, udpMove{flow: p, tag: tag, renew: !bad})
			}
		}
		// TCP streams crawling on a lossy node: close them so the app reconnects through
		// a healthy one. Only on degraded nodes, where silence is most likely the loss,
		// and on proxy nodes that just failed a probe and are being checked (the streams of a
		// node that is dying otherwise hang until the verdict), and only streams waiting for
		// an answer: a paused video with a full buffer is silent too, and closing it made the
		// player stall on resume. Direct and zapret's socks are left alone while merely
		// suspect: a probe they failed says little about a long poll. Not on a stuck node:
		// a group with nowhere else to go stays on it, and every LLM request thinking for 5s
		// there would have been cut for the minutes the verdict lasts.
		degraded := degradedLocked(n)
		var nodeFrozen []frozenConn
		for conn := range n.conns {
			nc, ok := conn.(*nodeConn)
			if !ok {
				continue
			}
			silent := nc.silentFor()
			if silent < frozenWith {
				continue
			}
			// auto-direct's first choice has its own answer to a cut, at cutIdle: the site moves
			// to the next exit, which closing it here first would pre-empt
			judged := nc.watchStalls && nc.onCut == nil
			switch {
			case judged && nc.frozenMid():
				nodeFrozen = append(nodeFrozen, frozenConn{conn: nc, tag: tag, silent: silent, how: frozenMidAnswer})
			case judged && nc.unanswered(now) >= frozenWith:
				nodeFrozen = append(nodeFrozen, frozenConn{conn: nc, tag: tag, silent: nc.unanswered(now), how: frozenUnanswered})
			case silent >= degradedStall && nc.awaitingAnswer() && (degraded || n.state == stateSuspect) && (degraded || nc.proxy):
				nc.setWhy(fmt.Sprintf("engine: no answer %.1fs on a %s node", silent.Seconds(), nodeTrouble(degraded, n.state)))
				stuck = append(stuck, nc)
			}
		}
		// See frozenWith: two streams of one node frozen at once are the node, one alone
		// gets a little longer to come back.
		for _, f := range nodeFrozen {
			if len(nodeFrozen) >= 2 || f.silent >= frozenAlone {
				f.together = len(nodeFrozen)
				frozen = append(frozen, f)
			}
		}
	}
	for tag, s := range r.nodeStats {
		down := s.bytesDown.Load()
		rate := down - s.lastDown
		s.lastDown = down
		if rate > s.peakBps || now.Sub(s.peakAt) > speedWindow {
			s.peakBps, s.peakAt = rate, now
		}
		// A node that was pouring data and then went completely quiet while its
		// connections are still open is checked at once rather than at the next probe
		// round: a node that dies mid-video looks exactly like this. A node where every
		// stream merely paused just passes the check.
		n := r.nodes[tag]
		if silenceStep(s, rate, n != nil && n.state != stateDead && len(n.conns) > 0) {
			r.urgent[tag] = quietCheck(n)
		}
	}
	r.access.Unlock()
	for _, m := range moves {
		if m.renew {
			go m.flow.renew(m.tag)
		} else {
			go m.flow.migrate(m.tag)
		}
	}
	for _, conn := range stuck {
		conn.closeForNode()
	}
	r.closeFrozen(frozen)
}

// quietCheck describes a node whose streams all went quiet at once: whether its pending
// reads wait on the node, or the copier waits on a client that stopped taking data. The
// second is the client side (a paused player, the client's own link), not the node.
func quietCheck(n *nodeHealth) string {
	onNode, onClient := 0, 0
	for conn := range n.conns {
		nc, ok := conn.(*nodeConn)
		if !ok {
			continue
		}
		if nc.silentFor() >= time.Second {
			onNode++
		} else if nc.reading.Load() == 0 {
			onClient++
		}
	}
	return fmt.Sprintf("streams went quiet at once (%d waiting on the node, %d on the client), checking now", onNode, onClient)
}

func nodeTrouble(degraded bool, state nodeState) string {
	if degraded {
		return "lossy"
	}
	return state.String()
}

// speed is what the node is known to deliver: the better of a recent active test and the
// peak seen on real traffic. Zero means unknown.
func (r *healthRegistry) speed(tag string) int64 {
	r.access.Lock()
	defer r.access.Unlock()
	s := r.nodeStats[tag]
	if s == nil {
		return 0
	}
	best := int64(0)
	if time.Since(s.peakAt) <= speedWindow {
		best = s.peakBps
	}
	if time.Since(s.speedAt) <= 2*speedWindow && s.speedSample > best {
		best = s.speedSample
	}
	return best
}

func (r *healthRegistry) setSpeedSample(tag string, bps int64) {
	r.access.Lock()
	s := r.statsLocked(tag)
	s.speedSample, s.speedAt = bps, time.Now()
	r.access.Unlock()
}

// setTestedSpeed records a finished speed test.
func (r *healthRegistry) setTestedSpeed(tag string, bps int64) {
	r.access.Lock()
	s := r.statsLocked(tag)
	s.speedSample, s.speedAt = bps, time.Now()
	s.testedBps, s.testedAt = bps, s.speedAt
	r.access.Unlock()
}

// testedSpeed is the result of a recent finished speed test, zero if there is none. Only a
// test can show a node too slow: the peak of real traffic is low whenever little went
// through it.
func (r *healthRegistry) testedSpeed(tag string) int64 {
	r.access.Lock()
	defer r.access.Unlock()
	s := r.nodeStats[tag]
	if s == nil || time.Since(s.testedAt) > 2*speedWindow {
		return 0
	}
	return s.testedBps
}

func (r *healthRegistry) speedSampleAge(tag string) time.Duration {
	r.access.Lock()
	defer r.access.Unlock()
	s := r.nodeStats[tag]
	if s == nil || s.speedAt.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	return time.Since(s.speedAt)
}
