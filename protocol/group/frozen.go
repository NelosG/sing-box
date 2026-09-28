package group

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Frozen streams. A stream stopped part way through an answer (inside a TLS record, which a
// server always finishes once it started it) with a read waiting on it is not a slow server:
// the path under it is gone. Left alone, the app waits out its own timeout, half a minute to
// two for a video player, then retries and finds the new connection fine. That is what
// production looked like: a node passing every probe, videos dead for a minute and back,
// and nothing in the logs, since a stall only counted once the stream carried on.
//
// Such a stream is closed after frozenAlone, or after frozenWith when another stream of the
// node froze with it; the app retries at once, and the race takes it elsewhere if the node
// is the problem. Two frozen streams within stuckWindow put the node out of the running as a
// primary for a while, whatever its probes say.
const (
	frozenWith  = 3 * time.Second
	frozenAlone = 5 * time.Second
	stuckWindow = 10 * time.Second
	stuckBase   = time.Minute
	stuckMax    = 10 * time.Minute
	// repeated verdicts double the time out; this is how fast that memory fades
	stuckHalfLife = 10 * time.Minute
	// how long a server's other protocols are passed over as a new primary after one of its
	// nodes died carrying traffic or froze
	siblingTrouble = time.Minute
)

type frozenConn struct {
	conn     *nodeConn
	tag      string
	silent   time.Duration
	how      string // frozenMidAnswer or frozenUnanswered
	together int
}

const (
	frozenMidAnswer  = "mid-answer"
	frozenUnanswered = "request unanswered" // see nodeConn.unanswered
)

type udpMove struct {
	flow  *migratingPacketConn
	tag   string
	renew bool
}

// closeFrozen ends frozen streams, counts them against their nodes and passes the verdict.
func (r *healthRegistry) closeFrozen(frozen []frozenConn) {
	for _, f := range frozen {
		c := f.conn
		c.setWhy(fmt.Sprintf("engine: frozen, %s %.1fs", f.how, f.silent.Seconds()))
		c.stats.real.streamStalls.add(1, realHalfLife)
		c.stats.stalls.Add(1)
		c.stats.stallMs.Add(f.silent.Milliseconds())
		detail := fmt.Sprintf("%s %s %.1fs, closed", c.site, f.how, f.silent.Seconds())
		if f.together > 1 {
			detail += fmt.Sprintf(" (%d streams of the node at once)", f.together)
		}
		r.eventLimited("frozen", f.tag, c.group, detail)
		// A stuck verdict takes the node away from every group, so only exits whose trouble is
		// their own count: a proxy or zapret's socks. Direct serves every site in the country,
		// and one frozen stream there is that site or its CDN.
		if c.proxy || c.socks {
			r.noteFreeze(f.tag, c.site, f.how == frozenMidAnswer, c.socks)
		}
		if c.onFrozen != nil {
			c.onFrozen()
		}
		c.closeForNode()
	}
}

// eventLimited logs at most one frozen event per node every stallEventGap; the counters take all.
func (r *healthRegistry) eventLimited(kind, tag, group, detail string) {
	s := r.stats(tag)
	now := time.Now().UnixNano()
	last := s.lastFrozenEvent.Load()
	if now-last < int64(stallEventGap) || !s.lastFrozenEvent.CompareAndSwap(last, now) {
		return
	}
	r.event(kind, tag, group, detail)
}

// noteFreeze counts a frozen stream of tag and marks the node stuck on two within stuckWindow
// that are both mid-answer, or that were on different sites. An unanswered request alone is the
// weaker sign (a slow backend after a big answer looks the same), and two of those on one site
// are that site; on two sites at once they are the node. zapret's socks (local) is out for
// stuckBase each time, not longer: it is the exit its groups exist for, and TSPU moods pass.
func (r *healthRegistry) noteFreeze(tag, site string, mid, local bool) {
	now := time.Now()
	r.access.Lock()
	n := r.node(tag)
	recent := n.freezes[:0]
	for _, f := range n.freezes {
		if now.Sub(f.at) < stuckWindow {
			recent = append(recent, f)
		}
	}
	n.freezes = append(recent, freezeMark{at: now, site: site, mid: mid})
	if !stuckEvidence(n.freezes) || now.Before(n.stuckUntil) || n.state == stateDead {
		r.access.Unlock()
		return
	}
	n.freezes = nil
	s := r.statsLocked(tag)
	repeats := s.real.stucks.get(stuckHalfLife)
	s.real.stucks.add(1, stuckHalfLife)
	out := time.Duration(math.Min(float64(stuckBase)*math.Exp2(math.Round(repeats)), float64(stuckMax)))
	if local {
		out = stuckBase
	}
	n.stuckUntil = now.Add(out)
	r.troubled[siblingKey(tag)] = now.Add(max(siblingTrouble, out))
	// checked at once too: a node whose traffic froze may be dying, and the probes decide that
	r.urgent[tag] = ""
	state := n.state
	r.access.Unlock()
	r.event("stuck", tag, "", fmt.Sprintf("real traffic froze (last: %s), out as a primary for %s", site, out.Round(time.Second)))
	r.notify(tag, state)
}

func stuckEvidence(freezes []freezeMark) bool {
	mids := 0
	sites := map[string]bool{}
	for _, f := range freezes {
		if f.mid {
			mids++
		}
		sites[f.site] = true
	}
	return mids >= 2 || len(freezes) >= 2 && len(sites) >= 2
}

// stuck reports a node out of the running as a primary because its real traffic froze.
func (r *healthRegistry) stuck(tag string) bool {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	return n != nil && time.Now().Before(n.stuckUntil)
}

// siblingKey names the server behind a node. The provider names every protocol of one server
// alike ("FI #3 | VLESS", "FI #3 | TROJAN", "FI #3 | HY2"), and they share its address.
func siblingKey(tag string) string {
	if i := strings.LastIndex(tag, " | "); i > 0 {
		return tag[:i]
	}
	return tag
}

// siblingTroubled reports a node whose server just lost another of its nodes to death or a
// frozen stream; the node itself is judged by its own state.
func (r *healthRegistry) siblingTroubled(tag string) bool {
	key := siblingKey(tag)
	if key == tag {
		return false
	}
	r.access.Lock()
	defer r.access.Unlock()
	until, ok := r.troubled[key]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(r.troubled, key)
		return false
	}
	return true
}

// resetOnce reports, once per death of tag, that its transport should be dropped: a node
// declared dead on a wedged QUIC connection kept failing its retries on that very
// connection, when a fresh handshake would have worked.
func (r *healthRegistry) resetOnce(tag string) bool {
	deaths := r.stats(tag).deaths.Load()
	r.access.Lock()
	defer r.access.Unlock()
	if r.resetAt[tag] == deaths {
		return false
	}
	r.resetAt[tag] = deaths
	return true
}
