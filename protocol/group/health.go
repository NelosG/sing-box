package group

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
)

// Node health shared by every urltest group of one box instance. Upstream keeps a single
// delay per tag and only learns about failures on the next probe round; here every probe
// and every real connection feeds a per-node state machine, so a group can drop a node the
// moment it stops answering.

type nodeState uint8

const (
	stateUnknown nodeState = iota
	stateAlive
	stateSuspect
	stateDead
)

func (s nodeState) String() string {
	switch s {
	case stateAlive:
		return "alive"
	case stateSuspect:
		return "suspect"
	case stateDead:
		return "dead"
	}
	return "unknown"
}

type probeKey struct {
	tag  string
	link string
}

type probeResult struct {
	at     time.Time
	rtt    float64 // EWMA, ms
	last   uint16
	status int
	// recent probe times for the median and spread, see score.go
	window [linkWindow]uint16
	filled int
	next   int
}

type nodeHealth struct {
	state      nodeState
	fails      int
	lastOK     time.Time
	stateSince time.Time
	conns      map[net.Conn]struct{}
	packets    map[*migratingPacketConn]struct{}
	// egress country as seen by a geo service through the node; the node address often
	// says nothing about it (cascades, anycast)
	country   string
	countryAt time.Time
	// last time anything checked the node: a probe attempt on any link
	lastAttempt time.Time
	// failed probes since the node died; spaces out retries of long-dead nodes
	deadFails int
	// outcomes of the last scheduled probes, newest in bit 0 (1 = failed); verify probes
	// are left out so that one outage does not count three times
	probeLog   uint8
	probeCount int
	// the last qualityWindow scheduled probes, newest in bit 0: stalled (slow but answered)
	// and failed ones, for the score
	qualStall uint16
	qualFail  uint16
	qualCount int
	// when the node last came back from the dead, and the scheduled probes it passed in a
	// row since any failure: how a revived node earns its place back
	revivedAt time.Time
	okStreak  int
	// real traffic froze on the node, see frozenSweep: out of the running as a primary until
	// then, whatever its probes say; freezes are the recent frozen streams that decide it
	stuckUntil time.Time
	freezes    []freezeMark
}

// freezeMark is one frozen stream of a node; mid: stopped inside a TLS record, the strong kind.
type freezeMark struct {
	at   time.Time
	site string
	mid  bool
}

func (r *healthRegistry) Country(tag string) (string, time.Time) {
	r.access.Lock()
	defer r.access.Unlock()
	if n := r.nodes[tag]; n != nil {
		return n.country, n.countryAt
	}
	return "", time.Time{}
}

func (r *healthRegistry) setCountry(tag, country string) {
	r.access.Lock()
	n := r.node(tag)
	changed := n.country != country
	n.country = country
	n.countryAt = time.Now()
	r.access.Unlock()
	if changed {
		r.notify(tag, r.State(tag))
	}
}

type healthListener func(tag string, state nodeState)

type healthRegistry struct {
	access    sync.Mutex
	nodes     map[string]*nodeHealth
	probes    map[probeKey]*probeResult
	inflight  map[probeKey]chan struct{}
	attempts  map[probeKey]time.Time
	listeners map[int]healthListener
	nextID    int
	refs      int
	probeSlot chan struct{}
	// egress checks are hourly and light; their own slots keep them from starving behind the
	// probe backlog of a start with a hundred nodes
	egressSlot chan struct{}
	nodeStats  map[string]*nodeStats
	urgent     map[string]string // nodes to check at once, and the event to log for it; see speedLoop
	events     eventLog
	flows      flowLog
	// servers whose node just died carrying traffic or froze, by siblingKey, and until when
	// the other protocols of that server are not taken as a new primary
	troubled map[string]time.Time
	// the death count at which a node's transport was last reset, see resetOnce
	resetAt map[string]int64
	closed  chan struct{}
	history *urltest.HistoryStorage
}

var (
	registriesAccess sync.Mutex
	registries       = map[*urltest.HistoryStorage]*healthRegistry{}
)

// acquireHealth returns the registry of the box that owns history; the history storage is
// already a per-box singleton, which makes it a convenient key.
func acquireHealth(history *urltest.HistoryStorage) *healthRegistry {
	registriesAccess.Lock()
	defer registriesAccess.Unlock()
	r := registries[history]
	if r == nil {
		r = &healthRegistry{
			nodes:      map[string]*nodeHealth{},
			probes:     map[probeKey]*probeResult{},
			inflight:   map[probeKey]chan struct{}{},
			attempts:   map[probeKey]time.Time{},
			probeSlot:  make(chan struct{}, maxConcurrentProbes),
			egressSlot: make(chan struct{}, maxConcurrentEgress),
			nodeStats:  map[string]*nodeStats{},
			urgent:     map[string]string{},
			troubled:   map[string]time.Time{},
			resetAt:    map[string]int64{},
			closed:     make(chan struct{}),
			listeners:  map[int]healthListener{},
			history:    history,
		}
		registries[history] = r
		go r.speedLoop(r.closed)
	}
	r.refs++
	return r
}

func (r *healthRegistry) release() {
	registriesAccess.Lock()
	defer registriesAccess.Unlock()
	r.refs--
	if r.refs == 0 {
		delete(registries, r.history)
		close(r.closed)
	}
}

func (r *healthRegistry) node(tag string) *nodeHealth {
	n := r.nodes[tag]
	if n == nil {
		n = &nodeHealth{conns: map[net.Conn]struct{}{}, packets: map[*migratingPacketConn]struct{}{}}
		r.nodes[tag] = n
	}
	return n
}

func (r *healthRegistry) subscribe(l healthListener) int {
	r.access.Lock()
	defer r.access.Unlock()
	r.nextID++
	r.listeners[r.nextID] = l
	return r.nextID
}

func (r *healthRegistry) unsubscribe(id int) {
	r.access.Lock()
	defer r.access.Unlock()
	delete(r.listeners, id)
}

func (r *healthRegistry) State(tag string) nodeState {
	r.access.Lock()
	defer r.access.Unlock()
	if n := r.nodes[tag]; n != nil {
		return n.state
	}
	return stateUnknown
}

// RTT returns the smoothed probe time of tag against link, or 0 when unknown.
func (r *healthRegistry) RTT(tag, link string) float64 {
	r.access.Lock()
	defer r.access.Unlock()
	if p := r.probes[probeKey{tag, link}]; p != nil {
		return p.rtt
	}
	return 0
}

func (r *healthRegistry) probeAge(tag, link string) time.Duration {
	r.access.Lock()
	defer r.access.Unlock()
	// A failed probe leaves no result, so the attempt time is what spaces out retries.
	last := r.attempts[probeKey{tag, link}]
	if p := r.probes[probeKey{tag, link}]; p != nil && p.at.After(last) {
		last = p.at
	}
	if last.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	return time.Since(last)
}

// Success records a working probe or connection. link may be empty for traffic signals.
func (r *healthRegistry) Success(tag, link string, ms uint16, status int) {
	r.success(tag, link, ms, status, false)
}

// resetDelay records a measurement that replaces the smoothed delay instead of blending in.
func (r *healthRegistry) resetDelay(tag, link string, ms uint16, status int) {
	r.success(tag, link, ms, status, true)
}

func (r *healthRegistry) success(tag, link string, ms uint16, status int, replace bool) {
	r.access.Lock()
	n := r.node(tag)
	n.fails = 0
	n.lastOK = time.Now()
	if link != "" {
		key := probeKey{tag, link}
		p := r.probes[key]
		if p == nil {
			p = &probeResult{rtt: float64(ms)}
			r.probes[key] = p
		} else if replace {
			// The average restarts from the fresh measurement; the window keeps its history,
			// the median already shrugs off one inflated sample and the spread must survive.
			p.rtt = float64(ms)
		} else {
			// Weight the newest sample by a third: one lucky probe cannot hide a degraded node,
			// and a node that recovered is trusted again within a few probes.
			p.rtt = p.rtt*2/3 + float64(ms)/3
		}
		p.push(ms)
		p.at = time.Now()
		p.last = ms
		p.status = status
	}
	previous, since := n.state, n.stateSince
	changed := r.setState(n, stateAlive)
	if changed && previous == stateDead {
		// The failures that led to the death say nothing about the node that came back, and
		// kept in the log they held a revived zapret out as lossy for another five minutes.
		n.probeLog, n.probeCount = 0, 0
		n.qualStall, n.qualFail, n.qualCount = 0, 0, 0
		n.revivedAt, n.okStreak = time.Now(), 0
	}
	r.access.Unlock()
	if changed && (previous == stateDead || previous == stateSuspect) {
		r.event("alive", tag, "", fmt.Sprintf("was %s %.1fs", previous, time.Since(since).Seconds()))
	}
	if link != "" {
		r.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: ms})
	}
	if changed {
		r.notify(tag, stateAlive)
	}
}

// Failure records a failed probe or connection attempt and returns the resulting state.
// deadAfter is how many consecutive failures turn a node dead.
func (r *healthRegistry) Failure(tag string, deadAfter int) nodeState {
	return r.failWith(tag, deadAfter, "")
}

// failWith is Failure with what failed, which goes into the suspect and dead events: with
// only the way back logged, a node flipping to suspect every minute said nothing about why.
func (r *healthRegistry) failWith(tag string, deadAfter int, why string) nodeState {
	r.access.Lock()
	n := r.node(tag)
	n.fails++
	next := stateSuspect
	if n.fails >= deadAfter || n.state == stateDead {
		next = stateDead
	}
	if n.state == stateDead {
		n.deadFails++
	}
	previous, since := n.state, n.stateSince
	changed := r.setState(n, next)
	if changed && next == stateDead && len(n.conns)+len(n.packets) > 0 {
		// A node that dies while carrying traffic takes its server's other protocols down with
		// it more often than not (FI#3 VLESS, TROJAN and HY2 on one host failed together, and
		// lb-meta hopped between them for an hour). A node dying unused says less: dead SS
		// nodes flap all day next to working siblings.
		r.troubled[siblingKey(tag)] = time.Now().Add(siblingTrouble)
	}
	r.access.Unlock()
	if changed {
		if next == stateDead {
			r.stats(tag).deaths.Add(1)
			r.stats(tag).real.flaps.add(1, flapHalfLife)
			detail := why
			if previous == stateSuspect {
				detail = fmt.Sprintf("suspect %.1fs, %s", time.Since(since).Seconds(), why)
			}
			r.event("dead", tag, "", detail)
			r.history.DeleteURLTestHistory(tag)
			go r.reap(tag)
		} else {
			r.event("suspect", tag, "", why)
		}
		r.notify(tag, next)
	}
	return next
}

// Every group probes on its own schedule, and at start each probes all members; with a
// dozen groups on different links that used to open hundreds of connections to the
// provider at once. All probes of a box share this many slots.
const maxConcurrentProbes = 24

// acquireProbeSlot blocks until a probe may run; it returns nil if done closed first.
func (r *healthRegistry) acquireProbeSlot(done <-chan struct{}) func() {
	select {
	case r.probeSlot <- struct{}{}:
		return func() { <-r.probeSlot }
	case <-done:
		return nil
	}
}

const maxConcurrentEgress = 4

func (r *healthRegistry) acquireEgressSlot(done <-chan struct{}) func() {
	select {
	case r.egressSlot <- struct{}{}:
		return func() { <-r.egressSlot }
	case <-done:
		return nil
	}
}

// probedAgo is the time since any probe of the node started, ignoring traffic.
func (r *healthRegistry) probedAgo(tag string) time.Duration {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil || n.lastAttempt.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	return time.Since(n.lastAttempt)
}

// nodeAge is the time since the node was last known to work or was last probed on any link.
func (r *healthRegistry) nodeAge(tag string) time.Duration {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil {
		return time.Duration(1<<63 - 1)
	}
	last := n.lastOK
	if n.lastAttempt.After(last) {
		last = n.lastAttempt
	}
	if last.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	return time.Since(last)
}

// deadRetryDue spaces out probes of a dead node: 15s right after it died, doubling with
// every failed retry up to 10 minutes. A node that has been down for days costs next to
// nothing, a node that just blinked is back within seconds.
func (r *healthRegistry) deadRetryDue(tag string, base time.Duration) bool {
	r.access.Lock()
	n := r.nodes[tag]
	var fails int
	if n != nil {
		fails = n.deadFails
	}
	r.access.Unlock()
	wait := base
	for i := 0; i < fails && wait < maxDeadRetry; i++ {
		wait *= 2
	}
	return r.nodeAge(tag) >= min(wait, maxDeadRetry)
}

const maxDeadRetry = 10 * time.Minute

// revival reports how long ago tag came back from the dead (zero if it never died or is not
// alive) and how many scheduled probes it has passed in a row since.
func (r *healthRegistry) revival(tag string) (time.Duration, int) {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil || n.state == stateDead || n.revivedAt.IsZero() {
		return 0, 0
	}
	return time.Since(n.revivedAt), n.okStreak
}

// aliveFor is how long tag has been alive without interruption, zero if it is not alive.
func (r *healthRegistry) aliveFor(tag string) time.Duration {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil || n.state != stateAlive {
		return 0
	}
	return time.Since(n.stateSince)
}

// Degraded: a node that fails a good share of its scheduled probes while never quite dying
// (heavy packet loss). It passes enough probes to stay alive, so without this it would keep
// its streams and stay primary while they crawl.
const (
	degradedWindow   = 8
	degradedFailures = 3
)

// recordProbe keeps the outcome of one scheduled probe; stall marks one that answered but
// took far longer than the link usually does.
func (r *healthRegistry) recordProbe(tag string, ok, stall bool) {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.node(tag)
	if n.state == stateDead {
		// the retries of a dead node: its revival starts the log afresh
		return
	}
	if ok {
		n.okStreak++
	} else {
		n.okStreak = 0
	}
	n.probeLog <<= 1
	n.qualStall <<= 1
	n.qualFail <<= 1
	if !ok {
		n.probeLog |= 1
		n.qualFail |= 1
	} else if stall {
		n.qualStall |= 1
	}
	n.probeCount = min(n.probeCount+1, degradedWindow)
	n.qualCount = min(n.qualCount+1, qualityWindow)
}

// isStall reports whether a probe of tag against link that took ms is a stall, judged by
// the link's own recent times.
func (r *healthRegistry) isStall(tag, link string, ms uint16) bool {
	r.access.Lock()
	defer r.access.Unlock()
	p := r.probes[probeKey{tag, link}]
	if p == nil {
		return float64(ms) > 3*stallFloor
	}
	return float64(ms) > p.stallThreshold()
}

func (r *healthRegistry) degraded(tag string) bool {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	return n != nil && n.state != stateDead && degradedLocked(n)
}

func degradedLocked(n *nodeHealth) bool {
	failures := 0
	for i := 0; i < n.probeCount; i++ {
		failures += int(n.probeLog >> i & 1)
	}
	return failures >= degradedFailures
}

// forget drops everything known about tag, for a node whose settings were replaced.
func (r *healthRegistry) forget(tag string) {
	r.access.Lock()
	if n := r.nodes[tag]; n != nil {
		n.state, n.fails, n.country, n.countryAt = stateUnknown, 0, "", time.Time{}
		n.probeLog, n.probeCount = 0, 0
		n.qualStall, n.qualFail, n.qualCount = 0, 0, 0
	}
	for key := range r.probes {
		if key.tag == tag {
			delete(r.probes, key)
		}
	}
	r.access.Unlock()
	r.history.DeleteURLTestHistory(tag)
}

// Streams on a dead node cannot be moved, so they are closed and the apps reconnect at
// once instead of hanging until TCP gives up minutes later. Only streams that have gone
// quiet for 3s are touched: data still arriving proves the node works for that stream, and a
// false verdict must never cut a live transfer. UDP flows are moved instead of closed.
const (
	reapIdle   = 3 * time.Second
	reapWindow = 30 * time.Second
	reapTick   = 250 * time.Millisecond
)

func (r *healthRegistry) reap(tag string) {
	deadline := time.Now().Add(reapWindow)
	for time.Now().Before(deadline) && r.State(tag) == stateDead {
		var conns []*nodeConn
		var packets []*migratingPacketConn
		r.access.Lock()
		if n := r.nodes[tag]; n != nil {
			for c := range n.conns {
				if nc, ok := c.(*nodeConn); ok && nc.idleFor() >= reapIdle {
					conns = append(conns, nc)
				}
			}
			for p := range n.packets {
				if p.idleFor() >= reapIdle {
					packets = append(packets, p)
				}
			}
		}
		r.access.Unlock()
		for _, c := range conns {
			c.setWhy("engine: node dead")
			c.closeForNode()
		}
		for _, p := range packets {
			p.migrate(tag)
		}
		time.Sleep(reapTick)
	}
}

func (r *healthRegistry) setState(n *nodeHealth, s nodeState) bool {
	if n.state == s {
		return false
	}
	if s != stateDead {
		n.deadFails = 0
	}
	n.state = s
	n.stateSince = time.Now()
	return true
}

func (r *healthRegistry) notify(tag string, s nodeState) {
	r.access.Lock()
	listeners := make([]healthListener, 0, len(r.listeners))
	for _, l := range r.listeners {
		listeners = append(listeners, l)
	}
	r.access.Unlock()
	for _, l := range listeners {
		l(tag, s)
	}
}

func (r *healthRegistry) trackConn(tag string, c net.Conn) {
	r.access.Lock()
	r.node(tag).conns[c] = struct{}{}
	r.access.Unlock()
}

// nodeConns lists the tracked connections through tag.
func (r *healthRegistry) nodeConns(tag string) []*nodeConn {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil {
		return nil
	}
	result := make([]*nodeConn, 0, len(n.conns))
	for c := range n.conns {
		if nc, ok := c.(*nodeConn); ok {
			result = append(result, nc)
		}
	}
	return result
}

func (r *healthRegistry) nodePackets(tag string) []*migratingPacketConn {
	r.access.Lock()
	defer r.access.Unlock()
	n := r.nodes[tag]
	if n == nil {
		return nil
	}
	result := make([]*migratingPacketConn, 0, len(n.packets))
	for p := range n.packets {
		result = append(result, p)
	}
	return result
}

func (r *healthRegistry) untrackConn(tag string, c net.Conn) {
	r.access.Lock()
	if n := r.nodes[tag]; n != nil {
		delete(n.conns, c)
	}
	r.access.Unlock()
}

func (r *healthRegistry) trackPacket(tag string, p *migratingPacketConn) {
	r.access.Lock()
	r.node(tag).packets[p] = struct{}{}
	r.access.Unlock()
}

func (r *healthRegistry) untrackPacket(tag string, p *migratingPacketConn) {
	r.access.Lock()
	if n := r.nodes[tag]; n != nil {
		delete(n.packets, p)
	}
	r.access.Unlock()
}

// HealthNode is the exported view of one node for panels.
type HealthNode struct {
	Tag         string               `json:"tag"`
	State       string               `json:"state"`
	StateSince  int64                `json:"state_since"`
	LastOK      int64                `json:"last_ok"`
	Fails       int                  `json:"fails"`
	Connections int                  `json:"connections"`
	UDPFlows    int                  `json:"udp_flows"`
	Country     string               `json:"country,omitempty"`
	Probes      map[string]HealthRTT `json:"probes"`
	BytesDown   int64                `json:"bytes_down"`
	BytesUp     int64                `json:"bytes_up"`
	RaceWins    int64                `json:"race_wins"`
	RaceLosses  int64                `json:"race_losses"`
	DialFails   int64                `json:"dial_fails"`
	ProbeOK     int64                `json:"probe_ok"`
	ProbeFail   int64                `json:"probe_fail"`
	Deaths      int64                `json:"deaths"`
	TTFB        int64                `json:"ttfb_ms"`
	Speed       int64                `json:"speed_bps"`
	Stalls      int64                `json:"stalls"`
	UDPStalls   int64                `json:"udp_stalls"`
	StallMs     int64                `json:"stall_ms"`
}

type HealthRTT struct {
	RTT    uint16 `json:"rtt"`
	Last   uint16 `json:"last"`
	Status int    `json:"status"`
	At     int64  `json:"at"`
	// the score a group on this link ranks the node by, and its parts
	Score  int    `json:"score"`
	Median int    `json:"p50"`
	P90    int    `json:"p90"`
	Why    string `json:"score_detail,omitempty"`
}

type HealthGroup struct {
	Tag           string   `json:"tag"`
	Link          string   `json:"link"`
	Primary       string   `json:"primary"`
	Members       []string `json:"members"`
	ExcludeEgress []string `json:"exclude_egress,omitempty"`
	Expected      []int    `json:"expected_status,omitempty"`
}

type HealthSnapshot struct {
	Nodes  []HealthNode  `json:"nodes"`
	Groups []HealthGroup `json:"groups"`
	Events []healthEvent `json:"events"`
}

var (
	groupsAccess sync.Mutex
	liveGroups   = map[*smartGroup]struct{}{}
)

// Snapshot reports node health and group choices of the box that owns history.
func Snapshot(history *urltest.HistoryStorage) HealthSnapshot {
	var snap HealthSnapshot
	registriesAccess.Lock()
	r := registries[history]
	registriesAccess.Unlock()
	if r == nil {
		return snap
	}
	r.access.Lock()
	for tag, n := range r.nodes {
		hn := HealthNode{Tag: tag, State: n.state.String(), Fails: n.fails, Connections: len(n.conns), UDPFlows: len(n.packets), Country: n.country, Probes: map[string]HealthRTT{}}
		if !n.stateSince.IsZero() {
			hn.StateSince = n.stateSince.UnixMilli()
		}
		if !n.lastOK.IsZero() {
			hn.LastOK = n.lastOK.UnixMilli()
		}
		snap.Nodes = append(snap.Nodes, hn)
	}
	for key, p := range r.probes {
		for i := range snap.Nodes {
			if snap.Nodes[i].Tag == key.tag {
				score, _ := r.scoreLocked(key.tag, key.link)
				median, p90, _ := p.quantiles()
				snap.Nodes[i].Probes[key.link] = HealthRTT{
					RTT: uint16(p.rtt), Last: p.last, Status: p.status, At: p.at.UnixMilli(),
					Score: int(score.total()), Median: int(median), P90: int(p90), Why: score.String(),
				}
			}
		}
	}
	r.access.Unlock()
	for i := range snap.Nodes {
		n := &snap.Nodes[i]
		s := r.stats(n.Tag)
		n.BytesDown, n.BytesUp = s.bytesDown.Load(), s.bytesUp.Load()
		n.RaceWins, n.RaceLosses, n.DialFails = s.raceWins.Load(), s.raceLosses.Load(), s.dialFails.Load()
		n.ProbeOK, n.ProbeFail, n.Deaths = s.probeOK.Load(), s.probeFail.Load(), s.deaths.Load()
		n.TTFB = s.ttfb.Load() / 1000
		n.Speed = r.speed(n.Tag)
		n.Stalls, n.UDPStalls, n.StallMs = s.stalls.Load(), s.udpStalls.Load(), s.stallMs.Load()
	}
	snap.Events = r.events.list()
	sort.Slice(snap.Nodes, func(i, j int) bool { return snap.Nodes[i].Tag < snap.Nodes[j].Tag })
	groupsAccess.Lock()
	for g := range liveGroups {
		if g.health != r {
			continue
		}
		hg := HealthGroup{Tag: g.tag, Link: g.link, Primary: g.Now(), ExcludeEgress: g.exclude, Expected: g.expected}
		hg.Members = append(hg.Members, g.tags...)
		snap.Groups = append(snap.Groups, hg)
	}
	groupsAccess.Unlock()
	sort.Slice(snap.Groups, func(i, j int) bool { return snap.Groups[i].Tag < snap.Groups[j].Tag })
	return snap
}

// beginProbe deduplicates concurrent probes of the same node and link across groups.
// It returns nil when another probe is already running; the caller may wait on the channel.
func (r *healthRegistry) beginProbe(key probeKey) (done func(), wait <-chan struct{}) {
	r.access.Lock()
	defer r.access.Unlock()
	if ch, running := r.inflight[key]; running {
		return nil, ch
	}
	ch := make(chan struct{})
	r.inflight[key] = ch
	r.attempts[key] = time.Now()
	if key.link != "egress" {
		r.node(key.tag).lastAttempt = time.Now()
	}
	return func() {
		r.access.Lock()
		delete(r.inflight, key)
		r.access.Unlock()
		close(ch)
	}, nil
}
