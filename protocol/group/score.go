package group

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"
)

// A group ranks its usable members by a score: what a user would feel on the node, in
// milliseconds. It is the node's typical probe time to the group's link plus penalties for
// the ways a node hurts while it is still "alive": stalls, failed probes, losing races on
// real traffic, connection errors, dying and coming back.
//
// The typical time is a median, so one slow probe does not move it; the mean used before let
// a single sample from the start-up burst decide. The penalties carry what a mean hides: FI#3
// answered in 0.4s most of the time and stalled 1.5-3s a few times a minute, which is exactly
// what freezes a video, while Latvia never stalled at a slightly worse median.
//
// Real-traffic counters decay with time and are compared against a prior, so a node that
// carries no traffic is treated as average rather than as perfect: otherwise the primary,
// the only node with real-traffic data, would always look worse than the untested rest.

const (
	linkWindow    = 16 // recent probe times kept per node and link
	qualityWindow = 16 // recent scheduled probes per node judged for stalls and failures
	// Stall and failure shares are pulled towards a normal rate by this many pseudo-probes,
	// and only the excess costs: right after the production start one failed probe among a
	// node's first eight cost 375 ms, more than the tolerance, and moved most groups off FI#3.
	qualityPrior = 16.0
	stallPrior   = 0.03
	failPrior    = 0.01

	stallFloor = 1000.0 // ms; a probe this slow and 3x the link median is a stall
	stallCost  = 2000.0 // ms per stall share: 1 stall in 16 probes costs 125 ms
	failCost   = 3000.0 // ms per failed-probe share
	tailWeight = 0.25   // share of the p90-p50 spread added to the median

	realHalfLife = 10 * time.Minute
	// Pseudo-observations at the prior rate. Only the primary carries traffic, so only it
	// collects real-traffic evidence; thin evidence must not make it look worse than the
	// untried rest and swing the group back and forth.
	realPrior = 40.0
	lostPrior = 0.02 // share of races a healthy primary loses to a backup started later
	lostCost  = 2000.0
	errPrior  = 0.01 // share of attempts a healthy node fails where another succeeds
	errCost   = 3000.0
	// Deaths: the first one in a while is free, repeated ones cost. Every death used to cost
	// a second with a 30-minute half-life, and a node that blinked once and was back stayed
	// out of the latency groups for an hour or two while being the best.
	flapHalfLife = 30 * time.Minute
	flapCost     = 1000.0 // ms per recent death beyond the first
	flapCap      = 3.0
	// Streams that got going (stallStreak bytes in a run) and then froze mid-answer before
	// carrying on: a video freezing half-way is exactly this, and probes never see it. Only
	// the node carrying traffic collects this, so the prior is generous: a normal rate must
	// not make the primary look worse than untried nodes and rotate the group between equals.
	streamStallPrior = 0.05
	streamStallCost  = 2000.0
)

// decayed is a counter whose value halves every half-life.
type decayed struct {
	access sync.Mutex
	value  float64
	at     time.Time
}

func (d *decayed) add(x float64, halfLife time.Duration) {
	d.access.Lock()
	defer d.access.Unlock()
	d.value = d.decayLocked(halfLife) + x
	d.at = time.Now()
}

func (d *decayed) get(halfLife time.Duration) float64 {
	d.access.Lock()
	defer d.access.Unlock()
	return d.decayLocked(halfLife)
}

func (d *decayed) decayLocked(halfLife time.Duration) float64 {
	if d.at.IsZero() {
		return 0
	}
	return d.value * math.Exp2(-float64(time.Since(d.at))/float64(halfLife))
}

// realTraffic are the decayed counters a node earns from connections it carried.
type realTraffic struct {
	primaryTries decayed // races in which the node was tried first
	primaryLost  decayed // of those, won by a backup that started later
	attemptsOK   decayed
	attemptsErr  decayed // failed while another node got through
	flaps        decayed // deaths
	streams      decayed // TCP streams that got going
	streamStalls decayed // of those, stalls mid-answer and frozen streams
	stucks       decayed // stuck verdicts, see noteFreeze
}

func (p *probeResult) push(ms uint16) {
	p.window[p.next] = ms
	p.next = (p.next + 1) % linkWindow
	p.filled = min(p.filled+1, linkWindow)
}

// quantiles returns the median and the 90th percentile of the recent probe times.
func (p *probeResult) quantiles() (float64, float64, int) {
	if p.filled == 0 {
		return p.rtt, p.rtt, 0
	}
	samples := make([]int, p.filled)
	for i := range samples {
		samples[i] = int(p.window[i])
	}
	sort.Ints(samples)
	return float64(samples[(len(samples)-1)/2]), float64(samples[(len(samples)-1)*9/10]), len(samples)
}

// stallThreshold is the probe time above which a probe of this link counts as a stall.
func (p *probeResult) stallThreshold() float64 {
	median, _, n := p.quantiles()
	if n < 3 {
		median = p.rtt
	}
	return math.Max(stallFloor, 3*median)
}

type nodeScore struct {
	base, tail, stall, fail, lost, errs, flap, streams float64
}

func (s nodeScore) total() float64 {
	return s.base + s.tail + s.stall + s.fail + s.lost + s.errs + s.flap + s.streams
}

func (s nodeScore) String() string {
	var parts []string
	add := func(name string, v float64) {
		if v >= 1 {
			parts = append(parts, fmt.Sprintf("%s +%.0f", name, v))
		}
	}
	add("tail", s.tail)
	add("stalls", s.stall)
	add("fails", s.fail)
	add("lost races", s.lost)
	add("errors", s.errs)
	add("flaps", s.flap)
	add("stream stalls", s.streams)
	out := fmt.Sprintf("%.0f ms", s.total())
	if len(parts) > 0 {
		out += " (median " + fmt.Sprintf("%.0f", s.base) + ", " + strings.Join(parts, ", ") + ")"
	}
	return out
}

// qualityShares returns how far the stall and failure shares of the node's recent scheduled
// probes sit above normal.
func qualityShares(n *nodeHealth) (float64, float64) {
	if n == nil || n.qualCount == 0 {
		return 0, 0
	}
	mask := uint16(1<<n.qualCount - 1)
	if n.qualCount >= 16 {
		mask = 0xffff
	}
	count := float64(n.qualCount)
	stalls := float64(bits.OnesCount16(n.qualStall & mask))
	fails := float64(bits.OnesCount16(n.qualFail & mask))
	return excess(stalls, count, stallPrior, qualityPrior), excess(fails, count, failPrior, qualityPrior)
}

// excess is how far an observed rate sits above the prior, with pseudo observations at the
// prior rate pulling thin evidence towards it.
func excess(events, trials, prior, pseudo float64) float64 {
	rate := (events + prior*pseudo) / (trials + pseudo)
	return math.Max(0, rate-prior)
}

// scoreLocked scores tag for link; r.access must be held. It returns nil when the pair
// was never probed successfully.
func (r *healthRegistry) scoreLocked(tag, link string) (nodeScore, *probeResult) {
	p := r.probes[probeKey{tag, link}]
	if p == nil {
		return nodeScore{}, nil
	}
	var s nodeScore
	median, p90, n := p.quantiles()
	if n < 3 {
		s.base = p.rtt
	} else {
		s.base = median
		s.tail = tailWeight * math.Max(0, p90-median)
	}
	stall, fail := qualityShares(r.nodes[tag])
	s.stall, s.fail = stall*stallCost, fail*failCost
	real := &r.statsLocked(tag).real
	s.lost = lostCost * excess(real.primaryLost.get(realHalfLife), real.primaryTries.get(realHalfLife), lostPrior, realPrior)
	ok, errs := real.attemptsOK.get(realHalfLife), real.attemptsErr.get(realHalfLife)
	s.errs = errCost * excess(errs, ok+errs, errPrior, realPrior)
	s.flap = flapCost * math.Min(math.Max(0, real.flaps.get(flapHalfLife)-1), flapCap)
	s.streams = streamStallCost * excess(real.streamStalls.get(realHalfLife), real.streams.get(realHalfLife), streamStallPrior, realPrior)
	return s, p
}
