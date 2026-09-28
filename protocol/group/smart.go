package group

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// smartGroup replaces the upstream urltest engine. The group config is unchanged, so an
// existing panel database keeps working; behavior differs where upstream hurts a relay
// with many flaky upstream nodes:
//
//   - a failed probe is re-checked at once, and a node that fails three times in a row is
//     dead within seconds instead of at the next interval;
//   - the primary node is probed every few seconds while the group carries traffic;
//   - a new TCP connection whose first bytes are safe to repeat (TLS ClientHello, or nothing
//     sent yet) goes to the primary, and if the primary has not answered within a short
//     hedge delay the same bytes are sent through the next node too; the first answer wins;
//   - selection changes never touch live connections; only a node that died has its TCP
//     streams closed, and its UDP flows are moved to another node in place.
type smartGroup struct {
	ctx          context.Context
	logger       log.ContextLogger
	tag          string
	manager      adapter.OutboundManager
	tags         []string
	link         string
	tolerance    float64
	expected     []int
	slow         time.Duration
	fast         time.Duration
	idle         time.Duration
	disableRace  bool
	exclude      []string
	egressURL    string
	minSpeed     int64
	speedURL     string
	fallback     bool
	siteTTL      time.Duration
	siteExplicit bool
	hedgeFixed   time.Duration
	// exits outside the group tried when none of its members answers, see resortOutbounds,
	// and the sites that answered only there
	lastResort  []string
	resortSites sync.Map // site -> siteChoice
	sites       sync.Map // site -> siteChoice
	strikes     sync.Map // site -> *siteStrikes
	slowNoted   sync.Map // site -> time.Time
	health      *healthRegistry
	subID       int

	access     sync.RWMutex
	primary    string
	lastSwitch time.Time
	// set after the first full probe round; before it the primary is simply whoever answered
	// first, and moving to the real best must not wait for the rate limit
	settled atomic.Bool
	// set once the group's own probes of the head of its list are back
	headProbed atomic.Bool
	// set for the reselect that closes the first probe round: a fallback group holds its
	// first answer during the round and takes the best once, here
	finalPick atomic.Bool
	// since when another member has beaten a fallback group's primary by the tolerance;
	// guarded by access
	betterSince time.Time
	// a switch made on delay alone waits for confirmSwitch; guarded by access
	confirming  bool
	urgent      bool
	lastConfirm time.Time
	confirmed   string
	confirmedAt time.Time
	// measure probes a node against the link; async runs the confirmation. Tests replace both.
	measure func(out adapter.Outbound) (uint16, int, error)
	async   func(func())

	seenAccess sync.Mutex
	seen       map[string]adapter.Outbound

	// recent UDP flow failures per destination, see flowWorthMoving
	flowAccess sync.Mutex
	flowFails  map[string][]time.Time

	lastActive  atomic.Int64
	wake        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	probeSem    chan struct{}
	verifying   sync.Map
	egressTried sync.Map
}

const (
	deadAfterFailures = 3
	deadRetryInterval = 15 * time.Second
	egressRefresh     = time.Hour
	speedRefresh      = 20 * time.Minute
	speedMargin       = 1.5
	// bytes per second above which nodes count as equally fast: a short test cannot tell
	// 150 from 300 Mbit/s apart, and no video needs the difference
	speedCeiling     = int64(100 * 1000 * 1000 / 8)
	optimizeInterval = 5 * time.Minute
	linkRefresh      = 10 * time.Minute
	// A revived local head (zapret, direct) takes its fallback group back after this many
	// scheduled probes in a row and no sooner than this; a revived node is probed at the fast
	// interval for revivalWatch so that takes seconds, not the minutes it took at the slow one.
	headReturnProbes = 5
	headReturnAfter  = 15 * time.Second
	headReturnLatest = time.Minute
	revivalWatch     = time.Minute
	// A head that died once lately is back after two good probes: zapret revived after a
	// strategy switch and the group sat on a foreign node for another minute. The longer
	// wait above is kept for a head that keeps blinking.
	headQuickProbes = 2
	headQuickAfter  = 3 * time.Second
	headQuickLatest = 20 * time.Second
	headBlinks      = 1.5 // decayed deaths (flapHalfLife) from which a head counts as blinking
	// see the dead-node branch of loop
	keyRetryInterval = 3 * time.Second
	// A fallback group moves to a member better than its primary by the tolerance for this
	// long, at most once per fallbackDriftEvery: lb-meta keeps one address, not a bad one.
	fallbackDrift      = 10 * time.Minute
	fallbackDriftEvery = time.Hour
	defaultSiteTTL     = 3 * time.Hour
	verifySpacing      = time.Second
	activeWindow       = 15 * time.Minute
	maxRaceAttempts    = 3
	maxSerialAttempts  = 6
	raceDeadline       = 15 * time.Second
	// a site whose handshake takes longer than this directly counts as not answering
	observeDeadline = 5 * time.Second
	// a direct stream silent this long inside a TLS record is taken as cut
	cutIdle       = 4 * time.Second
	udpAnswerWait = 2 * time.Second
	// how long a strict site's remembered exit gets before the natural choice is tried anyway
	strictLastResort = 4 * time.Second
	// how long a site stays pinned back when the exit it was moved to was only slow to answer
	softPinTTL = 30 * time.Minute
	// last_resort: how long the members get before the last resorts are tried too, and how
	// long a site that answered only there goes there first
	resortAfter  = 4 * time.Second
	resortMemory = 30 * time.Minute
	// how often a group may re-measure before a delay-driven switch, and how long the result
	// stands
	confirmSpacing = 30 * time.Second
	confirmValid   = 10 * time.Second
)

type smartOptions struct {
	link        string
	interval    time.Duration
	tolerance   uint16
	idle        time.Duration
	fast        time.Duration
	expected    []int
	disableRace bool
	exclude     []string
	egressURL   string
	minSpeed    int
	speedURL    string
	mode        string
	siteTTL     time.Duration
	hedge       time.Duration
	lastResort  []string
}

func newSmartGroup(ctx context.Context, logger log.ContextLogger, tag string, history *urltest.HistoryStorage, manager adapter.OutboundManager, tags []string, o smartOptions) *smartGroup {
	slow := o.interval
	if slow <= 0 || slow > time.Minute {
		// Upstream intervals are sized for one expensive probe round per group; here
		// probes are deduplicated per node and link, so a minute is cheap and bounds how
		// stale a backup's delay can be when it is suddenly needed.
		slow = time.Minute
	}
	fast := o.fast
	if fast <= 0 {
		fast = 3 * time.Second
	}
	tolerance := float64(o.tolerance)
	if tolerance == 0 {
		tolerance = 50
	}
	g := &smartGroup{
		ctx:         ctx,
		logger:      logger,
		tag:         tag,
		manager:     manager,
		tags:        tags,
		seen:        map[string]adapter.Outbound{},
		flowFails:   map[string][]time.Time{},
		link:        o.link,
		tolerance:   tolerance,
		expected:    o.expected,
		slow:        slow,
		fast:        fast,
		idle:        o.idle,
		disableRace: o.disableRace,
		exclude:     o.exclude,
		egressURL:   o.egressURL,
		minSpeed:    int64(o.minSpeed) * 1000 * 1000 / 8,
		speedURL:    o.speedURL,
		fallback:    o.mode == "fallback",
		siteTTL:     o.siteTTL,
		hedgeFixed:  o.hedge,
		lastResort:  o.lastResort,
		health:      acquireHealth(history),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
		probeSem:    make(chan struct{}, 8),
	}
	g.siteExplicit = g.siteTTL > 0
	if g.siteTTL <= 0 {
		g.siteTTL = defaultSiteTTL
	}
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return probeLink(g.ctx, g.link, out, g.probeTimeout(out))
	}
	g.async = func(f func()) { go f() }
	if o.mode != "" && o.mode != "latency" && o.mode != "fallback" {
		logger.Warn("unknown urltest mode ", o.mode, ", using latency")
	}
	g.subID = g.health.subscribe(g.onHealth)
	groupsAccess.Lock()
	liveGroups[g] = struct{}{}
	groupsAccess.Unlock()
	return g
}

func (g *smartGroup) start() {
	g.touch()
	go g.loop()
}

func (g *smartGroup) Close() error {
	g.closeOnce.Do(func() {
		close(g.done)
		groupsAccess.Lock()
		delete(liveGroups, g)
		groupsAccess.Unlock()
		g.health.unsubscribe(g.subID)
		g.health.release()
	})
	return nil
}

func (g *smartGroup) touch() {
	g.lastActive.Store(time.Now().UnixNano())
}

func (g *smartGroup) active() bool {
	return time.Since(time.Unix(0, g.lastActive.Load())) < activeWindow
}

func (g *smartGroup) member(tag string) bool {
	return slices.Contains(g.tags, tag)
}

// members resolves the member tags now. Groups used to keep the objects they saw at start,
// so a node edited in the panel (new credentials after a rotation) kept serving the group
// with its old settings until the core restarted. A replaced node also starts from a clean
// health record: whatever the old credentials earned says nothing about the new ones.
func (g *smartGroup) members() []adapter.Outbound {
	result := make([]adapter.Outbound, 0, len(g.tags))
	var replaced []string
	g.seenAccess.Lock()
	for _, tag := range g.tags {
		out, loaded := g.manager.Outbound(tag)
		if !loaded {
			continue
		}
		if old, known := g.seen[tag]; known && old != out {
			replaced = append(replaced, tag)
		}
		g.seen[tag] = out
		result = append(result, out)
	}
	g.seenAccess.Unlock()
	for _, tag := range replaced {
		g.health.forget(tag)
		g.logger.Info("member ", tag, " replaced, health reset")
	}
	return result
}

func (g *smartGroup) onHealth(tag string, state nodeState) {
	if !g.member(tag) {
		return
	}
	g.reselect()
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// resetTransport drops the connection a dead proxy node keeps under all its streams (the QUIC
// connection of Hysteria2 and TUIC, a gRPC or WebSocket transport, a multiplexer), as a
// network change does, so its retries start a fresh handshake: on a wedged connection they
// failed on and on while a new one would have worked at once. Only once the first retry
// failed too: a death can come from one group's site timing out through a node that carries
// everyone else's downloads fine, and dropping the connection at once cut them all.
// It reports whether it did, and the node is then retried at once rather than after the backoff.
func (g *smartGroup) resetTransport(tag string) bool {
	out, loaded := g.manager.Outbound(tag)
	if !loaded || !proxyNode(out) {
		return false
	}
	listener, ok := out.(adapter.InterfaceUpdateListener)
	if !ok || !g.health.resetOnce(tag) {
		return false
	}
	g.logger.Info("dropping the transport of dead node ", tag)
	listener.InterfaceUpdated(g.ctx)
	return true
}

func (g *smartGroup) Now() string {
	g.access.RLock()
	defer g.access.RUnlock()
	return g.primary
}

// fit reports whether out may serve this group right now.
func (g *smartGroup) fit(out adapter.Outbound) (float64, bool) {
	return g.fitAs(out, false)
}

// fitAs judges out; asPrimary tolerates a suspect node. One failed probe used to push the
// primary out and swing the group's IP for new connections, only for the node to be fine
// a second later; the primary now leaves when it is dead, and until then the race covers
// connections it would fail.
func (g *smartGroup) fitAs(out adapter.Outbound, asPrimary bool) (float64, bool) {
	tag := out.Tag()
	switch g.health.State(tag) {
	case stateDead:
		return 0, false
	case stateSuspect:
		if !asPrimary {
			return 0, false
		}
	}
	if g.health.degraded(tag) {
		return 0, false
	}
	// Real traffic froze on it: probes pass there, the videos do not. A working primary is not
	// pushed out for its siblings' trouble, but no sibling takes the place of a node that failed.
	if g.health.stuck(tag) || !asPrimary && g.health.siblingTroubled(tag) {
		return 0, false
	}
	if len(g.exclude) > 0 {
		// Until the egress is known the node could be anywhere, so it only serves as a
		// last resort through the fallback path.
		country, _ := g.health.Country(tag)
		if country == "" || slices.Contains(g.exclude, country) {
			return 0, false
		}
	}
	if g.minSpeed > 0 && proxyNode(out) {
		// Unknown speed is no reason to refuse a node; a speed test that came out short is.
		// Not the peak of real traffic: right after a restart zapret's socks carried little
		// and lb-youtube dropped it as "slow". Local exits are never held to it.
		if speed := g.health.testedSpeed(tag); speed > 0 && speed < g.minSpeed {
			return 0, false
		}
	}
	g.health.access.Lock()
	score, p := g.health.scoreLocked(tag, g.link)
	status := 0
	if p != nil {
		status = p.status
	}
	g.health.access.Unlock()
	if p == nil {
		return 0, false
	}
	if len(g.expected) > 0 && !slices.Contains(g.expected, status) {
		return 0, false
	}
	return score.total(), true
}

// reselect keeps the current primary unless it became unfit or another node is faster by
// more than the tolerance. Earlier members win ties, as upstream does, so member order is
// the operator's priority list.
func (g *smartGroup) reselect() {
	g.access.Lock()
	from, to := g.reselectLocked()
	g.access.Unlock()
	if to != nil {
		g.confirmSwitch(from, to)
	}
}

// reselectLocked returns the primary and the challenger when a switch has to be confirmed
// first.
func (g *smartGroup) reselectLocked() (adapter.Outbound, adapter.Outbound) {
	members := g.members()
	if len(members) == 0 {
		return nil, nil
	}
	// Until the group's own probes of the head of its list are back, results other groups put
	// in the shared registry would hand it to whichever node they happened to probe first, and
	// the head would take it back a moment later: two IP changes while every client
	// reconnects after a restart. The first member not known dead holds until then.
	probing := !g.headProbed.Load()
	if probing && g.primary != "" && g.health.State(g.primary) != stateDead {
		return nil, nil
	}
	var best adapter.Outbound
	var bestRTT float64
	move := moveNone
	if g.fallback && !probing {
		best, move = g.fallbackPrimary(members)
	}
	// The incumbent only gets its tolerance once the first probe round is done; before that it
	// is just whoever answered first, and the list order (direct or zapret first on purpose)
	// must decide.
	for _, out := range members {
		if best != nil || !g.settled.Load() {
			break
		}
		if out.Tag() == g.primary {
			if rtt, ok := g.fitAs(out, true); ok {
				best, bestRTT = out, rtt
			}
		}
	}
	for _, out := range members {
		if g.fallback || probing {
			break
		}
		rtt, ok := g.fit(out)
		if !ok {
			continue
		}
		if best == nil || bestRTT > rtt+g.tolerance {
			best, bestRTT = out, rtt
		}
	}
	if best != nil && !g.fallback {
		best = g.fasterWithinTolerance(members, best, bestRTT)
	}
	if best == nil {
		// Nothing is known to work yet (cold start, or everything down): fall back to the
		// first member that is not known dead, one whose traffic froze or that loses packets
		// only when there is nothing else. The race covers the case where it is dead.
		for _, lenient := range []bool{false, true} {
			for _, out := range members {
				tag := out.Tag()
				if best == nil && g.health.State(tag) != stateDead && !g.excluded(out) &&
					(lenient || !g.health.stuck(tag) && !g.health.degraded(tag)) {
					best = out
				}
			}
		}
		if best == nil {
			for _, out := range members {
				if !g.excluded(out) {
					best = out
					break
				}
			}
		}
		if best == nil {
			best = members[0]
		}
	}
	if current := g.primaryOf(members); move != moveNone && best.Tag() != g.primary && current != nil {
		// A fallback group gives up a working primary only on a fresh measurement of both: a
		// minute's hiccup against an older good sample must not move the group for good.
		if g.confirmed != best.Tag() || time.Since(g.confirmedAt) > confirmValid {
			g.urgent = move == moveEscape
			return current, best
		}
		g.confirmed = ""
		g.lastSwitch = time.Now()
	}
	if current := g.primaryOf(members); best.Tag() != g.primary && !g.fallback && current != nil {
		settled := g.settled.Load()
		// Leaving a primary that still works is an optimisation, and doing it more often
		// than this swings the group's IP for no real gain. The first choice after start,
		// leaving a dead primary and fallback's return up the list are not limited.
		// Neither is leaving a primary that went bad without dying: a node losing 30% of its
		// packets still answers probes, only slowly, and the limit kept the group on it for
		// minutes while every thirteenth new connection failed. The re-measurement below
		// still keeps one slow probe from moving the group.
		curRTT, _ := g.fitAs(current, true)
		escape := curRTT > 2*bestRTT && curRTT > bestRTT+max(2*g.tolerance, 500)
		if settled && !escape && time.Since(g.lastSwitch) < optimizeInterval {
			return nil, nil
		}
		// Before settling, moving up to the head of the list is the list order deciding and
		// happens at once; moving down waits for the end of the first probe round, since a
		// first handshake in the start-up burst is slow for reasons that pass (VLESS-Reality
		// on the stand: 2 ms later, but enough to push the group off the head for good).
		if !settled && slices.Index(members, best) > slices.Index(members, current) {
			return nil, nil
		}
		if settled && (g.confirmed != best.Tag() || time.Since(g.confirmedAt) > confirmValid) {
			g.urgent = escape
			return current, best
		}
		g.confirmed = ""
		if settled {
			g.lastSwitch = time.Now()
		}
	}
	if best.Tag() != g.primary {
		old := g.primary
		if old == "" {
			old = "none"
		}
		detail := "from " + old
		// why the members listed above the new primary were passed over
		var skipped []string
		above := 0
		for _, out := range members {
			if out == best {
				break
			}
			above++
			if len(skipped) < 4 {
				skipped = append(skipped, out.Tag()+": "+g.unfitReason(out))
			}
		}
		if len(skipped) > 0 {
			detail += "; above: " + strings.Join(skipped, ", ")
			if above > len(skipped) {
				detail += " +" + strconv.Itoa(above-len(skipped)) + " more"
			}
		}
		g.logger.Info("primary ", old, " -> ", best.Tag(), " (", detail, ")")
		g.health.event("primary", best.Tag(), g.tag, detail)
		g.primary = best.Tag()
		// the drift clock was about the old primary
		g.betterSince = time.Time{}
	}
	return nil, nil
}

// unfitReason says in a word or two why fit rejects out, or what beat it.
func (g *smartGroup) unfitReason(out adapter.Outbound) string {
	tag := out.Tag()
	switch g.health.State(tag) {
	case stateDead:
		return "dead"
	case stateSuspect:
		return "suspect"
	}
	if g.health.degraded(tag) {
		return "lossy"
	}
	if g.health.stuck(tag) {
		return "stuck"
	}
	if g.health.siblingTroubled(tag) {
		return "server in trouble"
	}
	if len(g.exclude) > 0 {
		country, _ := g.health.Country(tag)
		if country == "" {
			return "egress unknown"
		}
		if slices.Contains(g.exclude, country) {
			return "egress " + country
		}
	}
	if g.minSpeed > 0 && proxyNode(out) {
		if speed := g.health.testedSpeed(tag); speed > 0 && speed < g.minSpeed {
			return "slow"
		}
	}
	g.health.access.Lock()
	score, p := g.health.scoreLocked(tag, g.link)
	status := 0
	if p != nil {
		status = p.status
	}
	g.health.access.Unlock()
	if p == nil {
		return "not probed"
	}
	if len(g.expected) > 0 && !slices.Contains(g.expected, status) {
		return "status " + strconv.Itoa(status)
	}
	return score.String()
}

// fasterWithinTolerance trades a little latency for bandwidth: among members whose delay
// is within the tolerance of the latency winner, the one with clearly more measured
// throughput takes over. The current primary keeps its place unless it is beaten by more
// than speedMargin, so small measurement noise does not move the group.
func (g *smartGroup) fasterWithinTolerance(members []adapter.Outbound, best adapter.Outbound, bestRTT float64) adapter.Outbound {
	// Direct and local exits (zapret socks) sit in a group on purpose and have no
	// comparable speed; a node with an unknown speed has nothing to compare either.
	if !proxyNode(best) {
		return best
	}
	chosen := best
	chosenSpeed := min(g.health.speed(best.Tag()), speedCeiling)
	if chosenSpeed == 0 {
		return best
	}
	for _, out := range members {
		if out == best || !proxyNode(out) {
			continue
		}
		rtt, ok := g.fit(out)
		if !ok || rtt > bestRTT+g.tolerance {
			continue
		}
		speed := min(g.health.speed(out.Tag()), speedCeiling)
		if speed > 0 && float64(speed) > float64(chosenSpeed)*speedMargin {
			chosen, chosenSpeed = out, speed
		}
	}
	return chosen
}

// proxyNode is a single remote proxy: one with a speed worth comparing and whose failure
// says something about itself rather than about the site.
func proxyNode(out adapter.Outbound) bool {
	switch out.Type() {
	case C.TypeDirect, C.TypeSOCKS, C.TypeHTTP, C.TypeURLTest, C.TypeSelector, C.TypeBlock:
		return false
	}
	return true
}

// fallbackPrimary keeps a fallback group on one exit and returns it, and what kind of move
// that is (see fallbackMove).
//
// A local exit at the head of the list (direct, zapret's socks, a nested group) is there on
// purpose and wins whenever it works; after it died it takes the group back once it passed
// headReturnProbes scheduled probes in a row, so a head that blinks does not drag the
// group's address back and forth. Proxy nodes stand in plain subscription order, so among
// them the list decides nothing: the group stays on its primary while that works, takes the
// best score (and the faster of close ones) when it has to move, leaves a primary far worse
// than the best at once, and one merely worse by the tolerance after fallbackDrift. Picked
// by list order, lb-meta and YouTube's backup sat on a node failing a tenth of its probes
// only because the provider lists it first.
func (g *smartGroup) fallbackPrimary(members []adapter.Outbound) (adapter.Outbound, fallbackMove) {
	var current adapter.Outbound
	var currentRTT float64
	for _, out := range members {
		if out.Tag() == g.primary {
			// fitAs, not fit: a suspect primary is still the primary, and dropping it handed
			// the group to another member on every lost probe and back on the next good one
			// (lb-ai flipped between two protocols of one node a few times a minute).
			if rtt, ok := g.fitAs(out, true); ok {
				current, currentRTT = out, rtt
			}
		}
	}
	head := members[0]
	localHead := !proxyNode(head)
	if localHead {
		if head == current {
			g.betterSince = time.Time{}
			return head, moveNone
		}
		if _, ok := g.fit(head); ok && (current == nil || g.headReturns(head)) {
			return head, moveNone
		}
	}
	var best adapter.Outbound
	var bestRTT float64
	for _, out := range members {
		if localHead && out == head {
			continue
		}
		if rtt, ok := g.fit(out); ok && (best == nil || rtt < bestRTT) {
			best, bestRTT = out, rtt
		}
	}
	// The comparisons below are against the latency winner; the faster node within its
	// tolerance is only where the group goes.
	target := best
	if best != nil {
		target = g.fasterWithinTolerance(members, best, bestRTT)
	}
	if current == nil {
		return target, moveNone
	}
	// Until the first probe round is done the primary is whoever answered first. The group
	// holds it meanwhile and takes the best once, when the round ends, instead of changing
	// its address with every answer while all clients reconnect after a restart.
	if !g.settled.Load() {
		if g.finalPick.Load() && best != nil && target != current && currentRTT > bestRTT+g.tolerance {
			return target, moveNone
		}
		return current, moveNone
	}
	if best == nil || target == current || best == current {
		g.betterSince = time.Time{}
		return current, moveNone
	}
	if currentRTT > 2*bestRTT && currentRTT > bestRTT+max(2*g.tolerance, 500) {
		return target, moveEscape
	}
	if currentRTT <= bestRTT+g.tolerance {
		g.betterSince = time.Time{}
		return current, moveNone
	}
	if g.betterSince.IsZero() {
		g.betterSince = time.Now()
	}
	if time.Since(g.betterSince) >= fallbackDrift && time.Since(g.lastSwitch) >= fallbackDriftEvery {
		return target, moveDrift
	}
	return current, moveNone
}

// How a fallback group's pick relates to its working primary: none (it stays, or the
// primary no longer works and the move is forced), an escape from a far worse primary, or a
// drift to a node better for a while. The last two are confirmed first; an escape is urgent.
type fallbackMove int

const (
	moveNone fallbackMove = iota
	moveEscape
	moveDrift
)

// headReturns reports whether a fallback group's local head may take the group back. The
// wait is for a head that actually went down: at start-up members answer their first probe
// in whatever order, and the head must win at once. After a death it needs a run of good
// probes, or simply a minute alive: the streak counts every group's probes of the node, and
// direct is probed on several links, one of which may fail through it now and then.
func (g *smartGroup) headReturns(head adapter.Outbound) bool {
	s := g.health.stats(head.Tag())
	if s.deaths.Load() == 0 {
		return true
	}
	since, streak := g.health.revival(head.Tag())
	if s.real.flaps.get(flapHalfLife) >= headBlinks {
		return since >= headReturnAfter && streak >= headReturnProbes || since >= headReturnLatest
	}
	return since >= headQuickAfter && streak >= headQuickProbes || since >= headQuickLatest
}

// excluded reports a member whose known egress country this group refuses. Such a node is
// never used, not even as a race backup: for AI traffic one request from a banned country
// is enough to get the account flagged.
func (g *smartGroup) excluded(out adapter.Outbound) bool {
	if len(g.exclude) == 0 {
		return false
	}
	country, _ := g.health.Country(out.Tag())
	return country != "" && slices.Contains(g.exclude, country)
}

// candidates lists members for a new connection: the primary first, then fit members by
// delay, then members in unknown or suspect state, dead ones last.
func (g *smartGroup) candidates(network string) []adapter.Outbound {
	g.access.RLock()
	primary := g.primary
	g.access.RUnlock()
	type scored struct {
		out  adapter.Outbound
		rank int
		rtt  float64
		pos  int
		// another protocol of the primary's server: a backup there shares the primary's
		// trouble more often than not
		sibling bool
	}
	primaryKey := siblingKey(primary)
	var list []scored
	for i, out := range g.members() {
		if !slices.Contains(out.Network(), network) || g.excluded(out) {
			continue
		}
		s := scored{out: out, pos: i, sibling: primaryKey != primary && out.Tag() != primary && siblingKey(out.Tag()) == primaryKey}
		if out.Tag() == primary {
			s.rank = -1
		} else if rtt, ok := g.fit(out); ok {
			s.rtt = rtt
		} else if g.health.State(out.Tag()) == stateDead {
			s.rank = 2
		} else {
			s.rank = 1
		}
		if s.rank >= 0 && len(g.exclude) > 0 {
			// Right after start most egress countries are still unknown; for a group that
			// excludes countries such a node is a last resort, never a race backup.
			if country, _ := g.health.Country(out.Tag()); country == "" {
				s.rank = 3
			}
		}
		list = append(list, s)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].rank != list[j].rank {
			return list[i].rank < list[j].rank
		}
		if list[i].rank == 0 {
			// a fallback group's local exits keep their place ahead of the proxies, which go
			// by score like everywhere else
			li, lj := g.fallback && !proxyNode(list[i].out), g.fallback && !proxyNode(list[j].out)
			if li != lj {
				return li
			}
			if list[i].sibling != list[j].sibling {
				return !list[i].sibling
			}
			if !li && list[i].rtt != list[j].rtt {
				return list[i].rtt < list[j].rtt
			}
		}
		return list[i].pos < list[j].pos
	})
	result := make([]adapter.Outbound, len(list))
	for i := range list {
		result[i] = list[i].out
	}
	return result
}

// hedgeDelay is how long the primary gets before the next node is tried in parallel. The
// probe delay covers node handshake plus a round trip to the site, which is about what the
// first answer to a ClientHello costs.
func (g *smartGroup) hedgeDelay(out adapter.Outbound) time.Duration {
	if g.hedgeFixed > 0 {
		return g.hedgeFixed
	}
	rtt := g.health.RTT(out.Tag(), g.link)
	if rtt == 0 {
		return 400 * time.Millisecond
	}
	d := time.Duration(rtt*1.5)*time.Millisecond + 50*time.Millisecond
	return min(max(d, 200*time.Millisecond), 2*time.Second)
}

// probeTimeout bounds a scheduled probe. A probe is a fresh node handshake plus a request,
// 0.3-0.5s on the provider's nodes, with stalls of 1.5-3s now and then while the node is
// fine (FI#3 on 2026-09-24: 30 of 30 answered, two over 1.5s). With a 2s floor every such
// stall was a failed probe: a primary went suspect every half minute, lossy after three, and
// groups and UDP flows hopped between nodes.
func (g *smartGroup) probeTimeout(out adapter.Outbound) time.Duration {
	rtt := g.health.RTT(out.Tag(), g.link)
	if rtt == 0 {
		return 4 * time.Second
	}
	d := time.Duration(rtt*4) * time.Millisecond
	return min(max(d, 3*time.Second), 6*time.Second)
}

// verifyTimeout bounds the re-probes that decide whether a suspect node is dead. Death closes
// the node's idle streams, so it must take a node that does not answer at all, not a slow
// one; a node that is really gone fails these just the same, only a few seconds later.
func (g *smartGroup) verifyTimeout(out adapter.Outbound) time.Duration {
	return min(max(g.probeTimeout(out)+2*time.Second, 5*time.Second), 8*time.Second)
}

// loop schedules probes: the primary and the next candidate often while the group is in
// use, everything else at the slow interval, dead nodes at a fixed retry interval.
func (g *smartGroup) loop() {
	g.probeAll()
	g.settled.Store(true)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	lastSweep := time.Now()
	lastReselect := time.Now()
	for {
		select {
		case <-g.done:
			return
		case <-ticker.C:
		case <-g.wake:
		}
		if g.siteAware() {
			g.closeCutStreams()
		}
		// State changes reselect at once; delays drift without any, and a node that got
		// clearly faster (or a degraded mark that cleared) would otherwise wait for one.
		if time.Since(lastReselect) >= 5*time.Second {
			lastReselect = time.Now()
			g.reselect()
		}
		if time.Since(lastSweep) > time.Minute {
			lastSweep = time.Now()
			g.sites.Range(func(key, value any) bool {
				if time.Now().After(value.(siteChoice).until) {
					g.sites.Delete(key)
				}
				return true
			})
			g.slowNoted.Range(func(key, value any) bool {
				if time.Since(value.(time.Time)) > time.Minute {
					g.slowNoted.Delete(key)
				}
				return true
			})
			g.flowAccess.Lock()
			for destination, times := range g.flowFails {
				if len(times) == 0 || time.Since(times[len(times)-1]) > flowFailWindow {
					delete(g.flowFails, destination)
				}
			}
			g.flowAccess.Unlock()
			g.strikes.Range(func(key, value any) bool {
				strikes := value.(*siteStrikes)
				strikes.access.Lock()
				stale := time.Since(strikes.first) > siteStrikeWindow
				strikes.access.Unlock()
				if stale {
					g.strikes.Delete(key)
				}
				return true
			})
		}
		cands := g.candidates("tcp")
		hot := map[string]bool{}
		if g.active() {
			for i := 0; i < len(cands) && i < 2; i++ {
				hot[cands[i].Tag()] = true
			}
		}
		if g.active() {
			// Only the nodes that could become primary soon are worth a download.
			// Direct and zapret's socks are never compared on speed.
			for i := 0; i < len(cands) && i < 3; i++ {
				if proxyNode(cands[i]) && g.health.speedSampleAge(cands[i].Tag()) >= speedRefresh {
					g.speedAsync(cands[i])
				}
			}
		}
		for _, out := range g.members() {
			tag := out.Tag()
			// A node whose streams all went quiet, or whose real traffic froze (logged as stuck
			// already, why is empty), is checked now rather than at its next probe. Silence alone
			// is judged on proxies only: zapret's socks carries bursty video that pauses by design.
			if why, ok := g.health.takeUrgent(tag); ok && (proxyNode(out) || why == "" && out.Type() == C.TypeSOCKS) {
				if why != "" {
					g.health.event("quiet_check", tag, g.tag, why)
				}
				g.verifyWith(out, g.probeTimeout(out))
			}
			due := g.slow
			if g.health.State(tag) == stateDead {
				switch {
				case !proxyNode(out):
					// The local exits a group exists for (zapret, direct, a nested group) are
					// retried on the group's own link every keyRetryInterval, never backing off.
					// Retried once per node with the doubling wait, zapret came back 7-14 minutes
					// after it was fixed: the wait had grown to minutes by then, and a failure on
					// the other zapret group's link doubled it for this one too. Not proxies at the
					// head of a list: the pool groups share their first node, and a dozen links
					// probing it every 3s while it is dead would hold the probe slots.
					if g.health.probeAge(tag, g.link) >= keyRetryInterval {
						g.probeAsync(out, true)
					}
				case g.health.deadRetryDue(tag, deadRetryInterval):
					// Recovery is a property of the node, so one retry per node covers every
					// group; per link it was several probes a second at a dead address.
					g.probeAsync(out, true)
				}
				continue
			}
			// The fast check is about the node being alive, not about this group's link:
			// any group's probe of it counts, so a primary shared by many groups is not
			// hammered once per link. Traffic that got through does not count: on a lossy node
			// some connections always do, and the probes are what reveal the loss.
			//
			// Everything else: liveness once per node per slow interval, and this group's own
			// delay to its link refreshed rarely. Per link and per minute it was a dozen probes
			// a second once a subscription filled the groups with a hundred nodes.
			// A node just back from the dead is watched closely for a minute: its place in the
			// groups depends on the probes it passes now.
			isHot := hot[tag]
			if since, _ := g.health.revival(tag); since > 0 && since < revivalWatch {
				isHot = true
			}
			switch {
			case isHot && g.health.probedAgo(tag) >= g.fast,
				!isHot && g.health.nodeAge(tag) >= due,
				g.health.probeAge(tag, g.link) >= linkRefresh:
				g.probeAsync(out, false)
			}
			if g.health.State(tag) != stateDead {
				if _, at := g.health.Country(tag); time.Since(at) >= egressRefresh {
					g.egressAsync(out)
				}
			}
		}
	}
}

// probeAll probes every member, the head of the list first: those are the group's intended
// choices (zapret, direct, the priority nodes), and on a start with a hundred members they
// would otherwise wait behind every other group's probes.
func (g *smartGroup) probeAll() {
	members := g.members()
	head := min(len(members), 3)
	run := func(list []adapter.Outbound, egress bool) {
		var wg sync.WaitGroup
		for _, out := range list {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g.probe(out, false)
				// A group that excludes countries cannot use a node before its egress is
				// known; for the head that must not wait behind a hundred other nodes.
				if egress && g.health.State(out.Tag()) != stateDead {
					if country, _ := g.health.Country(out.Tag()); country == "" {
						g.egress(out)
					}
				}
			}()
		}
		wg.Wait()
	}
	run(members[:head], len(g.exclude) > 0)
	g.headProbed.Store(true)
	g.reselect()
	run(members[head:], false)
	g.finalPick.Store(true)
	g.reselect()
	g.finalPick.Store(false)
}

// testAll probes every member now and returns the delays, for the Clash API delay test.
func (g *smartGroup) testAll() map[string]uint16 {
	g.probeAll()
	// Resolved before taking the registry lock: members() may call forget(), which takes it.
	members := g.members()
	result := map[string]uint16{}
	g.health.access.Lock()
	defer g.health.access.Unlock()
	for _, out := range members {
		if p := g.health.probes[probeKey{out.Tag(), g.link}]; p != nil && g.health.nodes[out.Tag()].state != stateDead {
			result[out.Tag()] = p.last
		}
	}
	return result
}

// egressAsync learns the egress country of out, at most once a minute per node when the
// geo service is failing, shared with other groups asking at the same time.
func (g *smartGroup) egressAsync(out adapter.Outbound) {
	tag := out.Tag()
	if last, ok := g.egressTried.Load(tag); ok && time.Since(last.(time.Time)) < time.Minute {
		return
	}
	g.egressTried.Store(tag, time.Now())
	finish, _ := g.health.beginProbe(probeKey{tag, "egress"})
	if finish == nil {
		return
	}
	go func() {
		defer finish()
		g.checkEgress(out)
	}()
}

// egress learns the egress country of out now, or waits for a check already running.
func (g *smartGroup) egress(out adapter.Outbound) {
	tag := out.Tag()
	g.egressTried.Store(tag, time.Now())
	finish, wait := g.health.beginProbe(probeKey{tag, "egress"})
	if finish == nil {
		select {
		case <-wait:
		case <-g.done:
		}
		return
	}
	defer finish()
	g.checkEgress(out)
}

func (g *smartGroup) checkEgress(out adapter.Outbound) {
	tag := out.Tag()
	release := g.health.acquireEgressSlot(g.done)
	if release == nil {
		return
	}
	country, err := probeEgress(g.ctx, g.egressURL, out, 10*time.Second)
	release()
	if err != nil {
		g.logger.Debug("egress of ", tag, " unknown: ", err)
		return
	}
	g.health.setCountry(tag, country)
}

func (g *smartGroup) speedAsync(out adapter.Outbound) {
	tag := out.Tag()
	finish, _ := g.health.beginProbe(probeKey{tag, "speed"})
	if finish == nil {
		return
	}
	// Recorded up front so a failing test is not retried on every tick.
	g.health.setSpeedSample(tag, g.health.speed(tag))
	go func() {
		defer finish()
		release := g.health.acquireProbeSlot(g.done)
		if release == nil {
			return
		}
		bps, err := probeSpeed(g.ctx, g.speedURL, out, 20*time.Second)
		release()
		if err != nil {
			g.logger.Debug("speed test of ", tag, " failed: ", err)
			return
		}
		g.health.setTestedSpeed(tag, bps)
	}()
}

func (g *smartGroup) probeAsync(out adapter.Outbound, retry bool) {
	select {
	case g.probeSem <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-g.probeSem }()
		g.probe(out, retry)
	}()
}

// probe runs one probe of out against the group link, shared with any group probing the
// same pair at the same moment.
// retry is set for the scheduled check of a dead node; other probes skip a node that died
// while they waited for a slot, since probing a dead address costs a full timeout.
func (g *smartGroup) probe(out adapter.Outbound, retry bool) {
	key := probeKey{out.Tag(), g.link}
	finish, wait := g.health.beginProbe(key)
	if finish == nil {
		select {
		case <-wait:
		case <-g.done:
		}
		return
	}
	defer finish()
	release := g.health.acquireProbeSlot(g.done)
	if release == nil {
		return
	}
	if !retry && g.health.State(out.Tag()) == stateDead {
		release()
		return
	}
	ms, status, err := probeLink(g.ctx, g.link, out, g.probeTimeout(out))
	release()
	g.health.recordProbe(out.Tag(), err == nil, err == nil && g.health.isStall(out.Tag(), g.link, ms))
	if err != nil {
		g.logger.Debug("probe ", out.Tag(), " failed: ", err)
		g.health.stats(out.Tag()).probeFail.Add(1)
		g.fail(out, "probe: "+shortErr(err))
		if retry && g.resetTransport(out.Tag()) {
			// once this probe is off the books, or the retry would just wait for it
			time.AfterFunc(time.Second, func() { g.probeAsync(out, true) })
		}
		return
	}
	g.health.stats(out.Tag()).probeOK.Add(1)
	g.health.Success(out.Tag(), g.link, ms, status)
}

// fail counts one failure and, while the node is only suspect, confirms or clears it with
// immediate parallel re-probes rather than waiting for the schedule.
func (g *smartGroup) fail(out adapter.Outbound, why string) {
	state := g.health.failWith(out.Tag(), deadAfterFailures, g.tag+" "+why)
	if state == stateSuspect {
		g.verify(out)
	}
}

// shortErr keeps the tail of an error chain, where the cause is, short enough for an event.
func shortErr(err error) string {
	s := err.Error()
	if len(s) > 90 {
		s = "..." + s[len(s)-87:]
	}
	return s
}

func (g *smartGroup) verify(out adapter.Outbound) {
	g.verifyWith(out, g.verifyTimeout(out))
}

// verifyWith re-probes out three times, staggered; timeout is shorter for a node whose
// streams all went quiet at once, since that already says more than one lost probe.
func (g *smartGroup) verifyWith(out adapter.Outbound, timeout time.Duration) {
	tag := out.Tag()
	if _, running := g.verifying.LoadOrStore(tag, struct{}{}); running {
		return
	}
	go func() {
		defer g.verifying.Delete(tag)
		var wg sync.WaitGroup
		// Enough parallel probes to reach a verdict in one round trip of the probe timeout,
		// whether or not earlier failures were already counted.
		for i := 0; i < deadAfterFailures; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Staggered: probes started together all land in the same latency spike,
				// and a spike must not pass for a dead node.
				select {
				case <-time.After(time.Duration(i) * verifySpacing):
				case <-g.done:
					return
				}
				release := g.health.acquireProbeSlot(g.done)
				if release == nil {
					return
				}
				ms, status, err := probeLink(g.ctx, g.link, out, timeout)
				release()
				if err != nil {
					g.health.failWith(tag, deadAfterFailures, g.tag+" re-check: "+shortErr(err))
					return
				}
				g.health.Success(tag, g.link, ms, status)
			}()
		}
		wg.Wait()
	}()
}

// ---- TCP ----

type attemptResult struct {
	out   adapter.Outbound
	conn  net.Conn
	first *buf.Buffer
	err   error
	ttfb  time.Duration
	// when the race launched it and how long it ran, for the flow record
	at, took time.Duration
	// the node handed over a connection; what failed after that was the site's answer
	connected bool
}

// nodeFault tells a failure to get through the node (no connection, handshake or tunnel
// error, a node that does not answer) from a site failing behind a node that works: an
// error after the node handed over the connection, or the node reporting that its own dial
// to the site failed ("remote error: dial tcp ... connection refused" from Hysteria2).
func nodeFault(r attemptResult) bool {
	if r.connected || r.err == nil {
		return false
	}
	// a TLS alert from the node's own handshake reads "remote error: tls: ..." and is the node's
	msg := r.err.Error()
	return !strings.Contains(msg, "remote error") || strings.Contains(msg, "remote error: tls:")
}

// raceSummary lists what a race tried, for the flow record: every finished attempt with
// its launch offset and outcome, and how many were still running when the winner was taken.
func raceSummary(winner *attemptResult, failed []attemptResult, running int) string {
	var parts []string
	for _, r := range failed {
		parts = append(parts, fmt.Sprintf("%s +%dms: %s after %dms", r.out.Tag(), r.at.Milliseconds(), shortErr(r.err), r.took.Milliseconds()))
	}
	if winner != nil {
		parts = append(parts, fmt.Sprintf("%s +%dms: ok after %dms", winner.out.Tag(), winner.at.Milliseconds(), winner.took.Milliseconds()))
	}
	if running > 0 {
		parts = append(parts, fmt.Sprintf("%d still running", running))
	}
	return strings.Join(parts, "; ")
}

// newConnection handles a routed TCP connection end to end.
func (g *smartGroup) newConnection(ctx context.Context, cm adapter.ConnectionManager, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	g.touch()
	payload := takeCachedPayload(conn)
	// Repeating bytes through a second node is only harmless when they carry no request of
	// their own: nothing at all, or a TLS ClientHello whose answer we pick from one node.
	safe := payload == nil || isTLSClientHello(payload)
	remote, err := g.dialRace(ctx, metadata, payload, safe && !g.disableRace, true)
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		g.logger.ErrorContext(ctx, E.Cause(err, "open connection to ", metadata.Destination, " using outbound/urltest[", g.tag, "]"))
		return
	}
	cm.NewConnection(ctx, &readyDialer{conn: remote}, conn, metadata, onClose)
}

// resortOutbounds are the group's last_resort exits that exist, carry network and are not
// known dead. A member listed there races as a member anyway.
func (g *smartGroup) resortOutbounds(network string) []adapter.Outbound {
	var result []adapter.Outbound
	for _, tag := range g.lastResort {
		if g.member(tag) || g.health.State(tag) == stateDead {
			continue
		}
		if out, loaded := g.manager.Outbound(tag); loaded && slices.Contains(out.Network(), network) {
			result = append(result, out)
		}
	}
	return result
}

// dialRace races the members for a connection. resortOK allows the last_resort exits: a
// routed connection, not a group dialed by the probe or by another group, whose own race
// decides when its members are out of answers.
func (g *smartGroup) dialRace(ctx context.Context, metadata adapter.InboundContext, payload []byte, hedge, resortOK bool) (net.Conn, error) {
	cands := g.candidates("tcp")
	if len(cands) == 0 {
		return nil, E.New("missing supported outbound")
	}
	site := siteKey(metadata)
	natural := cands[0]
	cands, strict := g.preferRemembered(site, cands)
	moved := cands[0] != natural
	// A site that answered only through a last resort goes there first while the memory
	// lasts, with the members raced behind it on the hedge timer: with the provider's nodes
	// all refused by the site (animego.me, in the RKN list yet open from Russia and closed
	// to foreign addresses) every connection waited resortAfter for nothing.
	var resort []adapter.Outbound
	var resortFirst adapter.Outbound
	if resortOK {
		resort = g.resortOutbounds(N.NetworkTCP)
		if value, found := g.resortSites.Load(site); found && time.Now().Before(value.(siteChoice).until) {
			for i, out := range resort {
				if out.Tag() == value.(siteChoice).tag {
					resortFirst = out
					cands = append([]adapter.Outbound{out}, cands...)
					resort = slices.Delete(resort, i, i+1)
					break
				}
			}
		}
	}
	limit := maxSerialAttempts
	if hedge {
		limit = max(limit, maxRaceAttempts)
	}
	limit = min(limit, len(cands))
	waitFirst := hedge && payload != nil
	results := make(chan attemptResult, limit+len(resort))
	raceCtx, cancelRace := context.WithTimeout(ctx, raceDeadline)
	defer cancelRace()
	// In fallback mode the natural first choice (direct) keeps trying after it loses the
	// race, so that a slow site is told apart from one that does not answer at all: only the
	// latter is moved to the next exit. Cut short with the race, both would look the same.
	siteAware := g.siteAware()
	observe := siteAware && !moved
	naturalCtx, cancelObserve := raceCtx, context.CancelFunc(func() {})
	if observe {
		naturalCtx, cancelObserve = context.WithTimeout(context.WithoutCancel(ctx), observeDeadline)
	}
	started, pending, inflight := 0, 0, 0
	raceStart := time.Now()
	launchOut := func(out adapter.Outbound) {
		pending++
		inflight++
		attemptCtx := raceCtx
		if out == natural {
			attemptCtx = naturalCtx
		}
		at := time.Since(raceStart)
		go func() {
			r := g.attempt(attemptCtx, out, metadata, payload, waitFirst)
			r.at, r.took = at, time.Since(raceStart)-at
			results <- r
		}()
	}
	launch := func() {
		started++
		launchOut(cands[started-1])
	}
	// The last resorts join after resortAfter without an answer, or at once when every
	// member tried has failed: all of them together, the first answer wins.
	resortLaunched := false
	launchResort := func() {
		if !resortLaunched {
			resortLaunched = true
			for _, out := range resort {
				launchOut(out)
			}
		}
	}
	var resortC <-chan time.Time
	if len(resort) > 0 {
		resortTimer := time.NewTimer(resortAfter)
		resortC = resortTimer.C
		defer resortTimer.Stop()
	}
	launch()
	var timer *time.Timer
	var timerC <-chan time.Time
	if hedge && started < limit {
		timer = time.NewTimer(g.hedgeDelay(cands[0]))
		timerC = timer.C
		defer timer.Stop()
	}
	// For a strict site the natural choice is not raced on the hedge timer, only when the
	// others failed or got nowhere for strictLastResort: a proxy that cannot reach the host
	// may hang until the race deadline instead of failing.
	var lastResortC <-chan time.Time
	if strict {
		lastResort := time.NewTimer(strictLastResort)
		lastResortC = lastResort.C
		defer lastResort.Stop()
	}
	var failed []attemptResult
	var winner *attemptResult
	naturalFailed, naturalDone := false, false
	for pending > 0 && winner == nil {
		select {
		case r := <-results:
			pending--
			inflight--
			if r.out == natural {
				naturalDone = true
				naturalFailed = r.err != nil
			}
			if r.err == nil {
				winner = &r
				break
			}
			failed = append(failed, r)
			if started < limit {
				launch()
				if timer != nil {
					timer.Reset(g.hedgeDelay(cands[started-1]))
				}
			}
			if pending == 0 {
				launchResort()
			}
		case <-resortC:
			launchResort()
		case <-timerC:
			if started < limit && started < maxRaceAttempts && !(strict && cands[started] == natural) {
				launch()
				timer.Reset(g.hedgeDelay(cands[started-1]))
			}
		case <-lastResortC:
			if started < limit {
				launch()
			}
		case <-raceCtx.Done():
			pending = 0
		}
	}
	var winnerOut adapter.Outbound
	if winner != nil {
		winnerOut = winner.out
	}
	// The natural choice lost only on speed if it is still running: whether the site moves
	// is decided when it finishes.
	decideLater := observe && winnerOut != nil && winnerOut != natural && !naturalDone
	// Losers still running, and anything still running when the race timed out, are closed
	// as they finish.
	if inflight > 0 {
		go func(n int) {
			defer cancelObserve()
			for i := 0; i < n; i++ {
				r := <-results
				if r.err == nil {
					r.conn.Close()
					r.first.Release()
				}
				if decideLater && r.out == natural {
					if r.err != nil {
						g.remember(site, natural, winnerOut)
					} else {
						g.noteSlow(site, natural)
					}
				}
			}
		}(inflight)
	} else {
		cancelObserve()
	}
	if winner == nil {
		g.health.flows.add(FlowRecord{Net: "tcp", Group: g.tag, Site: site, OpenMs: time.Since(raceStart).Milliseconds(),
			Tried: raceSummary(nil, failed, inflight), Close: "open failed"})
		// Every node failed: most likely the destination itself is down, which says
		// nothing about the nodes, so nobody is penalised.
		if len(failed) > 0 {
			return nil, failed[0].err
		}
		return nil, E.Cause(raceCtx.Err(), "no node answered")
	}
	// The group's own first choice, which the statistics and checks below are about: a
	// remembered last resort raced ahead of it is not a member, and probing the group's link
	// through direct would only have made direct suspect for every group.
	lead := cands[0]
	if resortFirst != nil && len(cands) > 1 {
		lead = cands[1]
	}
	resortWon := !g.member(winner.out.Tag())
	switch {
	case resortWon && site != "":
		if _, known := g.resortSites.Load(site); !known {
			g.health.event("site_fallback", winner.out.Tag(), g.tag, site+": no member answered, last resort did")
		}
		g.resortSites.Store(site, siteChoice{tag: winner.out.Tag(), until: time.Now().Add(resortMemory)})
	case resortFirst != nil:
		// a member answered the site again
		g.resortSites.Delete(site)
	}
	firstFailed := false
	for _, r := range failed {
		g.health.stats(r.out.Tag()).dialFails.Add(1)
		firstFailed = firstFailed || r.out == lead
		// Only a proxy node failing where another one got through says something about the
		// node. Direct, zapret's socks and nested groups fail per site (a blocked domain
		// resets direct at once), and their health is shared by every group: three blocked
		// requests in a row declared direct dead, auto-direct sent all unlisted traffic
		// abroad and idle direct streams were closed. Probes judge those exits.
		if !siteAware && proxyNode(r.out) {
			g.health.stats(r.out.Tag()).real.attemptsErr.add(1, realHalfLife)
			// Only a failure to get through the node at all makes it suspect. A site that
			// refused or hung up behind a working node is the site's matter, and each such
			// suspect spell closed every stream waiting on the node in every group and moved
			// its UDP flows: the two production primaries flipped every minute or two.
			if nodeFault(r) {
				g.fail(r.out, "connect to "+site+": "+shortErr(r.err))
			}
		}
	}
	// Real traffic for the score. A backup that started later and still answered first is a
	// head-to-head result on the very same site, so it is fair to the first choice whatever
	// the site; slow answers alone are not counted, a slow site makes every node look slow.
	if !siteAware && proxyNode(winner.out) {
		g.health.stats(winner.out.Tag()).real.attemptsOK.add(1, realHalfLife)
	}
	// Losing to zapret's socks or direct says nothing about a proxy: a local exit answers
	// sooner by nature, and the node's record is shared by every group it is in.
	if !siteAware && proxyNode(lead) && proxyNode(winner.out) {
		first := &g.health.stats(lead.Tag()).real
		first.primaryTries.add(1, realHalfLife)
		if started > 1 && winner.out != lead && !firstFailed {
			first.primaryLost.add(1, realHalfLife)
		}
	}
	switch {
	case strict && winner.out == natural:
		// The exit the site was moved to could not serve it while the first choice could: the
		// move was a mistake, pin the site back. When that exit only failed to answer within
		// strictLastResort rather than failing outright, it may just have been slow for a
		// moment (a node failing over), so the pin is short.
		ttl, why := g.siteTTL, "unreachable there"
		if !firstFailed {
			ttl, why = min(g.siteTTL, softPinTTL), "no answer there in time"
		}
		g.sites.Store(site, siteChoice{tag: natural.Tag(), until: time.Now().Add(ttl), pinned: true})
		g.health.event("site_restored", natural.Tag(), g.tag, site+" from "+lead.Tag()+": "+why)
	case resortWon:
	case !observe:
		g.remember(site, lead, winner.out)
	case winner.out != natural && naturalFailed:
		g.remember(site, natural, winner.out)
	}
	g.health.stats(winner.out.Tag()).raceWins.Add(1)
	if started > 1 && winner.out != lead && !resortWon {
		g.health.stats(lead.Tag()).raceLosses.Add(1)
	}
	if winner.ttfb > 0 {
		g.health.observeTTFB(winner.out.Tag(), winner.ttfb)
	}
	if winner.out != lead && !siteAware && resortFirst == nil {
		// The primary lost; find out now whether it is dying rather than at its next probe.
		g.verify(lead)
	}
	g.health.Success(winner.out.Tag(), "", 0, 0)
	// set up before the node's sweeps can see it, which read these fields every second
	tracked := untrackedNodeConn(g.health, winner.out.Tag(), winner.conn)
	tracked.site, tracked.group = site, g.tag
	// a nested group's conn wraps a node conn that records the same stall already
	tracked.watchStalls = winner.out.Type() != C.TypeURLTest && winner.out.Type() != C.TypeSelector
	tracked.proxy = proxyNode(winner.out)
	// Frozen streams count against zapret only where it is the head its group exists for
	// (YouTube): through auto-direct or as a last resort it carries any site TSPU may freeze,
	// and those would have put YouTube off zapret.
	tracked.socks = winner.out.Type() == C.TypeSOCKS && g.isFirstMember(winner.out)
	tracked.openMs, tracked.ttfbMs = time.Since(raceStart).Milliseconds(), winner.ttfb.Milliseconds()
	if started > 1 || len(failed) > 0 || time.Since(raceStart) >= flowSlowOpen {
		tracked.tried = raceSummary(winner, failed, inflight)
	}
	// Record boundaries tell a stream frozen part way through an answer from one that is
	// done and idle, on every exit; see frozen.
	tracked.tls = newTLSRecords()
	if winner.first != nil {
		// the server's first bytes were read by the race, before this wrapper
		tracked.tls.feed(winner.first.Bytes())
	}
	if siteAware && site != "" && len(cands) > 1 {
		switch {
		case !moved && winner.out == natural && g.isFirstMember(natural):
			// Silence is not judged for Russian names nor for bare addresses: an address
			// without a name is an app's own protocol more often than a web page, and in
			// production Yandex, Apple and AWS addresses were moved abroad for it. Nor for a
			// pinned site, whose move back showed the silence is the site's own; a cut in the
			// middle of a TLS record still counts there, or a wrong pin would leave a throttled
			// site on direct for hours.
			if !g.pinned(site) && !russianSite(site) && !ipSite(site) {
				tracked.onUnanswered = func() { g.siteUnanswered(site, natural) }
			}
			tracked.onCut = func() { g.siteCut(site, natural) }
			tracked.debugf = func(args ...any) { g.logger.Debug(append([]any{"site ", site, ": "}, args...)...) }
		case moved && winner.out == lead:
			// The same silence through the other exit means the site simply answers
			// like that; send it back and leave it alone for the memory time.
			alternative := winner.out
			tracked.onUnanswered = func() { g.siteMoveFailed(site, natural, alternative) }
			tracked.onFrozen = func() { g.siteFrozen(site, natural, alternative) }
		}
	}
	g.health.trackConn(winner.out.Tag(), tracked)
	if winner.first != nil && !winner.first.IsEmpty() {
		return bufio.NewCachedConn(tracked, winner.first), nil
	}
	return tracked, nil
}

func (g *smartGroup) attempt(ctx context.Context, out adapter.Outbound, metadata adapter.InboundContext, payload []byte, waitFirst bool) attemptResult {
	r := attemptResult{out: out}
	start := time.Now()
	// The winner outlives the race, so it must not be dialed with a context that the race
	// cancels; the race context only bounds how long we wait.
	dialCtx, cancelDial := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancelDial)
	var conn net.Conn
	var err error
	if len(metadata.DestinationAddresses) > 0 || metadata.Destination.IsIP() {
		conn, err = dialer.DialSerialNetwork(dialCtx, out, N.NetworkTCP, metadata.Destination, metadata.DestinationAddresses, metadata.NetworkStrategy, metadata.NetworkType, metadata.FallbackNetworkType, metadata.FallbackDelay)
	} else {
		conn, err = out.DialContext(dialCtx, N.NetworkTCP, metadata.Destination)
	}
	if err != nil {
		stop()
		r.err = err
		return r
	}
	r.connected = true
	abort := func(e error) attemptResult {
		stop()
		conn.Close()
		r.err = e
		return r
	}
	if payload != nil {
		if _, err = conn.Write(payload); err != nil {
			return abort(err)
		}
	}
	if waitFirst {
		// Closing the conn is the only portable way to interrupt a read on every node type.
		closeOnCancel := context.AfterFunc(ctx, func() { conn.Close() })
		buffer := buf.NewPacket()
		_, err = buffer.ReadOnceFrom(conn)
		if !closeOnCancel() {
			buffer.Release()
			return abort(E.Cause(ctx.Err(), "race lost"))
		}
		if err != nil {
			buffer.Release()
			return abort(err)
		}
		r.first = buffer
		r.ttfb = time.Since(start)
	}
	if !stop() {
		r.first.Release()
		return abort(E.Cause(ctx.Err(), "race lost"))
	}
	r.conn = conn
	return r
}

type siteChoice struct {
	tag   string
	until time.Time
	// pinned: kept on this exit after a move did not help; no new moves until expiry
	pinned bool
	// strict: moved because the first choice cut or starved the site after connecting; that
	// exit stays out of the race until expiry, since it wins the handshake every time
	strict bool
}

func ipSite(site string) bool {
	_, err := netip.ParseAddr(site)
	return err == nil
}

// russianSite: a name in a Russian zone. Such sites are not moved abroad for closing
// without an answer: from Russia that is their normal short reply far more often than a
// block (MTS, Ozon and M.Video APIs answering a tiny 204 and closing were all moved), and a
// Russian service behind a foreign IP may refuse outright. Blocked ones fail the connection
// instead (reset, timeout), and the race moves those.
func russianSite(site string) bool {
	site = strings.ToLower(strings.TrimSuffix(site, "."))
	for _, zone := range []string{"ru", "su", "xn--p1ai", "moscow", "xn--80adxhks"} {
		if site == zone || strings.HasSuffix(site, "."+zone) {
			return true
		}
	}
	return false
}

// siteKey names the destination for the per-site memory: the sniffed domain when there is
// one, since a site's addresses change and its name does not.
func siteKey(metadata adapter.InboundContext) string {
	if metadata.Domain != "" {
		return metadata.Domain
	}
	if metadata.Destination.IsFqdn() {
		return metadata.Destination.Fqdn
	}
	return metadata.Destination.AddrString()
}

// preferRemembered moves the exit that last worked for site to the front, while the memory
// is fresh and that exit is usable.
// preferRemembered also reports strict: the natural first choice was moved to the end and
// is tried only when everything before it failed.
func (g *smartGroup) preferRemembered(site string, cands []adapter.Outbound) ([]adapter.Outbound, bool) {
	if !g.siteAware() {
		return cands, false
	}
	value, found := g.sites.Load(site)
	if !found {
		return cands, false
	}
	choice := value.(siteChoice)
	if time.Now().After(choice.until) {
		// Expired: the first choice gets another chance, maybe the block was lifted.
		g.sites.Delete(site)
		return cands, false
	}
	for i, out := range cands {
		if out.Tag() == choice.tag && i > 0 && g.health.State(out.Tag()) != stateDead {
			reordered := append([]adapter.Outbound{out}, cands[:i]...)
			reordered = append(reordered, cands[i+1:]...)
			if !choice.strict {
				return reordered, false
			}
			// The first choice connects fine and then cuts the answer; racing it only lets it
			// win the handshake, take the memory back and stall the page again (msi.com's
			// cookie script on Cloudflare, every reload 10-20s). It stays as the last resort
			// though: a site moved by mistake that the other exit cannot reach at all was
			// dead for the whole memory time (Russian and Alibaba hosts, seen in production).
			var first []adapter.Outbound
			reordered = slices.DeleteFunc(reordered, func(o adapter.Outbound) bool {
				if g.isFirstMember(o) {
					first = append(first, o)
					return true
				}
				return false
			})
			return append(reordered, first...), len(first) > 0
		}
	}
	return cands, false
}

// siteUnanswered counts requests that the first choice let die without an answer (the
// site closes or resets right after the request, as a geo-fenced CDN or a DPI box does,
// while the handshake itself went fine). Two within siteStrikeWindow move the site to the
// next exit for the usual memory time.
func (g *smartGroup) siteUnanswered(site string, first adapter.Outbound) {
	now := time.Now()
	value, _ := g.strikes.LoadOrStore(site, &siteStrikes{})
	strikes := value.(*siteStrikes)
	strikes.access.Lock()
	if now.Sub(strikes.first) > siteStrikeWindow {
		strikes.first, strikes.count = now, 0
	}
	strikes.count++
	count := strikes.count
	strikes.access.Unlock()
	if count < siteStrikeLimit {
		return
	}
	g.strikes.Delete(site)
	for _, out := range g.candidates(N.NetworkTCP) {
		if out != first {
			g.sites.Store(site, siteChoice{tag: out.Tag(), until: now.Add(g.siteTTL), strict: true})
			g.health.event("site_moved", out.Tag(), g.tag, site+" from "+first.Tag()+": closed without answer")
			return
		}
	}
}

const flowFailWindow = time.Minute

// flowWorthMoving records a failed UDP flow to destination and says whether moving it may
// help: only the first failure within flowFailWindow does.
func (g *smartGroup) flowWorthMoving(destination string) bool {
	g.flowAccess.Lock()
	defer g.flowAccess.Unlock()
	now := time.Now()
	var recent []time.Time
	for _, t := range g.flowFails[destination] {
		if now.Sub(t) < flowFailWindow {
			recent = append(recent, t)
		}
	}
	recent = append(recent, now)
	g.flowFails[destination] = recent
	return len(recent) == 1
}

// primaryOf returns the current primary if it is still usable as primary.
func (g *smartGroup) primaryOf(members []adapter.Outbound) adapter.Outbound {
	for _, out := range members {
		if out.Tag() == g.primary {
			if _, ok := g.fitAs(out, true); ok {
				return out
			}
			return nil
		}
	}
	return nil
}

// confirmSwitch measures the primary and the challenger again, one right after the other,
// before a working primary is given up on delay. Stored delays are sampled at different
// moments, the first ones under the probe burst of a start: one sample of 871 ms on the
// head of the list against 91 ms taken later on another node moved most groups off it.
func (g *smartGroup) confirmSwitch(primary, challenger adapter.Outbound) {
	g.access.Lock()
	// An escape from a bad primary is not held back by the spacing: its re-measurement resets
	// the inflated delay, so a node that was only slow once does not ask again.
	if g.confirming || !g.urgent && time.Since(g.lastConfirm) < confirmSpacing {
		g.access.Unlock()
		return
	}
	g.confirming = true
	g.lastConfirm = time.Now()
	g.access.Unlock()
	g.async(func() {
		// A primary failing its re-measurement is one more reason to leave it; only a
		// challenger that fails its own keeps the group where it is. Requiring both let a
		// lossy primary veto its own replacement again and again.
		g.remeasure(primary)
		ok := g.remeasure(challenger)
		g.access.Lock()
		g.confirming = false
		if ok {
			g.confirmed, g.confirmedAt = challenger.Tag(), time.Now()
		}
		g.access.Unlock()
		if ok {
			g.reselect()
		}
	})
}

// remeasure takes the better of two back-to-back probes as the node's delay to the link,
// replacing the smoothed value rather than blending into it.
func (g *smartGroup) remeasure(out adapter.Outbound) bool {
	var best uint16
	var status int
	for i := 0; i < 2; i++ {
		release := g.health.acquireProbeSlot(g.done)
		if release == nil {
			return false
		}
		ms, st, err := g.measure(out)
		release()
		if err != nil {
			g.health.stats(out.Tag()).probeFail.Add(1)
			g.fail(out, "re-measure: "+shortErr(err))
			return false
		}
		g.health.stats(out.Tag()).probeOK.Add(1)
		if i == 0 || ms < best {
			best, status = ms, st
		}
	}
	g.health.resetDelay(out.Tag(), g.link, best, status)
	return true
}

// siteAware: per-site moves are for a group whose first member is direct (auto-direct), or
// one given site_memory explicitly. In a group of proxy nodes run in fallback mode the point
// is one steady exit; moving single sites between its nodes defeats it.
func (g *smartGroup) siteAware() bool {
	if !g.fallback {
		return false
	}
	if g.siteExplicit {
		return true
	}
	members := g.members()
	return len(members) > 0 && members[0].Type() == C.TypeDirect
}

// isFirstMember: silence is only judged on the group's first choice (direct in
// auto-direct). When that exit is down and a later one leads, moving sites is pointless.
func (g *smartGroup) isFirstMember(out adapter.Outbound) bool {
	members := g.members()
	return len(members) > 0 && members[0] == out
}

// noteSlow logs, at most once a minute per site, that direct lost a race on speed only.
func (g *smartGroup) noteSlow(site string, natural adapter.Outbound) {
	if last, found := g.slowNoted.Load(site); found && time.Since(last.(time.Time)) < time.Minute {
		return
	}
	g.slowNoted.Store(site, time.Now())
	g.health.event("site_slow", natural.Tag(), g.tag, site+" answered directly, only slower")
}

func (g *smartGroup) pinned(site string) bool {
	value, found := g.sites.Load(site)
	return found && value.(siteChoice).pinned && time.Now().Before(value.(siteChoice).until)
}

// siteCut moves a site at once: a stream cut in the middle of a TLS record is no mood of the
// server. The self-check still applies through the other exit.
func (g *smartGroup) siteCut(site string, natural adapter.Outbound) {
	for _, out := range g.candidates(N.NetworkTCP) {
		if out != natural {
			g.sites.Store(site, siteChoice{tag: out.Tag(), until: time.Now().Add(g.siteTTL), strict: true})
			g.health.event("site_moved", out.Tag(), g.tag, site+" from "+natural.Tag()+": cut mid-response")
			return
		}
	}
}

// closeCutStreams ends direct streams stuck inside a TLS record, so the browser retries at
// once (through the other exit now) instead of waiting minutes for its own timeout.
func (g *smartGroup) closeCutStreams() {
	members := g.members()
	if len(members) == 0 {
		return
	}
	for _, conn := range g.health.nodeConns(members[0].Tag()) {
		if conn.onCut != nil && conn.cutStalled(cutIdle) && conn.reportCut() {
			conn.setWhy("engine: cut mid-record, site moved")
			conn.Close()
		}
	}
	// A UDP flow through direct that was never answered (QUIC to a blocked site) moves to
	// the next exit instead of leaving the browser to time out and fall back to TCP.
	for _, flow := range g.health.nodePackets(members[0].Tag()) {
		if flow.g == g && flow.unanswered(udpAnswerWait) {
			go flow.migrate(members[0].Tag())
		}
	}
}

// siteMoveFailed undoes a move whose new exit gets the same silent closes: pinning the site
// to the natural exit stops it bouncing, and a site that is not blocked should not leave
// the country (a Russian bank behind a foreign exit may refuse to talk at all).
func (g *smartGroup) siteMoveFailed(site string, natural, alternative adapter.Outbound) {
	now := time.Now()
	value, _ := g.strikes.LoadOrStore(site, &siteStrikes{})
	strikes := value.(*siteStrikes)
	strikes.access.Lock()
	if now.Sub(strikes.first) > siteStrikeWindow {
		strikes.first, strikes.count = now, 0
	}
	strikes.count++
	count := strikes.count
	strikes.access.Unlock()
	if count < siteStrikeLimit {
		return
	}
	g.strikes.Delete(site)
	// With more exits after this one (auto-direct = direct, zapret, the pool) the site tries
	// the next before the silence is taken for the site's own manner: a site zapret cannot
	// get through may still answer through the pool.
	if next := g.exitAfter(alternative, natural); next != nil {
		g.sites.Store(site, siteChoice{tag: next.Tag(), until: now.Add(g.siteTTL), strict: true})
		g.health.event("site_moved", next.Tag(), g.tag, site+" from "+alternative.Tag()+": same silence there")
		return
	}
	g.sites.Store(site, siteChoice{tag: natural.Tag(), until: now.Add(g.siteTTL), pinned: true})
	g.health.event("site_restored", natural.Tag(), g.tag, site+" from "+alternative.Tag()+": same silence there")
}

// siteFrozen moves a site on at once when its stream froze mid-answer on the exit it was
// moved to, as a cut on the first choice does; on the last exit there is nowhere to go.
func (g *smartGroup) siteFrozen(site string, natural, alternative adapter.Outbound) {
	if next := g.exitAfter(alternative, natural); next != nil {
		g.sites.Store(site, siteChoice{tag: next.Tag(), until: time.Now().Add(g.siteTTL), strict: true})
		g.health.event("site_moved", next.Tag(), g.tag, site+" from "+alternative.Tag()+": frozen mid-answer")
	}
}

// exitAfter is the candidate that comes after after in the group's order, skipping skip.
func (g *smartGroup) exitAfter(after, skip adapter.Outbound) adapter.Outbound {
	cands := g.candidates(N.NetworkTCP)
	for i, out := range cands {
		if out != after {
			continue
		}
		for _, next := range cands[i+1:] {
			if next != skip {
				return next
			}
		}
		return nil
	}
	return nil
}

type siteStrikes struct {
	access sync.Mutex
	first  time.Time
	count  int
}

const (
	siteStrikeLimit  = 2
	siteStrikeWindow = time.Minute
)

// remember records which exit answered for site when it was not the one tried first.
func (g *smartGroup) remember(site string, first, winner adapter.Outbound) {
	if !g.siteAware() || site == "" || g.pinned(site) {
		return
	}
	if winner == first {
		return
	}
	value, found := g.sites.Load(site)
	current := found && time.Now().Before(value.(siteChoice).until)
	if current && value.(siteChoice).tag == winner.Tag() {
		return
	}
	// A site moved on down the chain (direct, zapret, the pool) because the first choice cuts
	// it stays strict, or direct would be raced again, win the handshake and cut it again.
	strict := current && value.(siteChoice).strict
	g.sites.Store(site, siteChoice{tag: winner.Tag(), until: time.Now().Add(g.siteTTL), strict: strict})
	g.health.event("site_moved", winner.Tag(), g.tag, site+" from "+first.Tag())
}

// takeCachedPayload consumes the bytes the sniffer already read from the client, so they
// can be written to more than one node.
func takeCachedPayload(conn net.Conn) []byte {
	reader, counters := N.UnwrapCountReader(conn, nil)
	cached, ok := reader.(N.CachedReader)
	if !ok {
		return nil
	}
	buffer := cached.ReadCached()
	if buffer == nil {
		return nil
	}
	defer buffer.Release()
	payload := append([]byte(nil), buffer.Bytes()...)
	for _, counter := range counters {
		counter(int64(len(payload)))
	}
	if len(payload) == 0 {
		return nil
	}
	return payload
}

func isTLSClientHello(b []byte) bool {
	// record type handshake, legacy version 3.x, handshake type client_hello
	return len(b) > 5 && b[0] == 0x16 && b[1] == 0x03 && b[5] == 0x01
}

// readyDialer hands an already established connection to the connection manager.
type readyDialer struct {
	conn net.Conn
	used atomic.Bool
}

func (d *readyDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	if d.used.Swap(true) {
		return nil, E.New("connection already used")
	}
	return d.conn, nil
}

func (d *readyDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, E.New("not a packet dialer")
}

// nodeConn registers a live connection under its node, so the node's death can close it.
// It deliberately does not expose the connection beneath it (no Upstream method): XTLS
// Vision looks through wrappers for a raw TCP socket and splices to it in the kernel, and
// every byte would then bypass the counters and the checks for silent or cut answers.
type nodeConn struct {
	net.Conn
	health   *healthRegistry
	tag      string
	stats    *nodeStats
	once     sync.Once
	lastRead atomic.Int64

	// Set for connections that went through a fallback group's first choice: called once
	// if the far end closes, resets or stalls right after a request without answering.
	onUnanswered func()
	// set for a site on the exit it was moved to: called when a stream of it froze, see frozen
	onFrozen       func()
	lastWrite      atomic.Int64
	downSinceWrite atomic.Int64
	judged         atomic.Bool
	// set when we close the conn ourselves: the read still pending in the other direction
	// then fails, and that is not the far end hanging up
	closing atomic.Bool
	// set when the close is about the node (dead, lossy), not about this conn's site
	quiet atomic.Bool

	// Also set only for a fallback group's first choice: record tracking to spot a stream
	// cut in the middle of a TLS record, and what to do about it.
	tlsAccess sync.Mutex
	tls       *tlsRecords
	onCut     func()
	debugf    func(args ...any)
	cut       atomic.Bool

	// stall telemetry: which site and group the conn serves, and the bytes of the current
	// run of data (only the reading goroutine touches streak)
	site, group string
	watchStalls bool
	streak      int64
	stall       pendingStall
	// carried by a remote proxy node rather than direct, zapret's socks or a nested group;
	// socks: carried by zapret's socks
	proxy, socks bool

	// when the pending Read began, 0 while none is pending. Time since the last data is not
	// the far end's silence: a client that stops reading (a paused video with a full buffer)
	// leaves the copier blocked writing to it, and nothing is read from the node meanwhile.
	// Taken for silence, that cut paused direct streams as "cut mid-response" and logged
	// minute-long stalls on busy nodes.
	reading atomic.Int64

	// the last run of data as the tick sees it: its bytes, and its rate in bytes per second
	// (runStart is when it began; reading goroutine only), and what the client sent since
	// the last data
	run, runRate atomic.Int64
	runStart     int64
	upSinceRead  atomic.Int64

	// for the flow record, see FlowRecord
	opened     time.Time
	openMs     int64
	ttfbMs     int64
	tried      string
	down, up   atomic.Int64
	midWait    atomic.Int64 // ns
	clientMax  atomic.Int64 // ns
	lastReturn atomic.Int64 // when the last Read returned
	stallCount atomic.Int32
	why        atomic.Pointer[string]
}

// the run of data after which a silence counts as the middle of an answer for a stream that
// is not TLS, where record boundaries cannot tell
const midStreak = 16 << 10

// setWhy notes why the flow ends; the first reason given stands.
func (c *nodeConn) setWhy(why string) {
	c.why.CompareAndSwap(nil, &why)
}

// midAnswer reports that the far end stopped part way through an answer rather than after
// one: inside a TLS record, which a server always finishes once it started it, or for a
// stream that is not TLS, after a run of data the client asked nothing new since.
func (c *nodeConn) midAnswer(prev int64) bool {
	if c.tls != nil {
		c.tlsAccess.Lock()
		valid, inside := c.tls.valid, c.tls.inside()
		c.tlsAccess.Unlock()
		if valid {
			return inside
		}
	}
	return c.run.Load() >= midStreak && c.lastWrite.Load() < prev+int64(ackGrace)
}

// frozenMid reports a TLS stream stopped inside a record, the only silence closed for being
// one: without record boundaries a finished burst looks the same as a cut one, and an SSH
// session sitting idle after a screenful of output would be killed.
func (c *nodeConn) frozenMid() bool {
	if c.tls == nil {
		return false
	}
	c.tlsAccess.Lock()
	defer c.tlsAccess.Unlock()
	return c.tls.valid && c.tls.inside()
}

// A stream counts as a video download when its last run of data was at least bulkRun at
// bulkRate or more: a CDN answers a request on such a connection in a fraction of a second.
// An LLM's event stream trickles at kilobytes a second however long it gets, and an API may
// think for many seconds before answering the next request on the connection; a request is
// small, an upload is not (a server may process an upload for long).
// The request must also come while the answer was flowing or right after it (a seek on a
// playing video): an API call made after a big page load comes later, and may take seconds.
const (
	bulkRun      = 1 << 20
	bulkRate     = 1 << 20
	smallRequest = 16 << 10
	askedWithin  = 2 * time.Second
)

// unanswered is how long the client has waited on a video download for any answer to the
// small request it sent after the last data (a range request after a seek, an HTTP/2 PING),
// with a read pending all the while; zero when that is not the case. Writes within ackGrace
// of the last data are HTTP/2 window updates acknowledging it, not requests. This catches a
// stream frozen on a record boundary, which frozenMid cannot tell from a finished answer:
// Google sends records of one packet early in a connection, and a player that seeks asks
// again on the very connection that froze.
func (c *nodeConn) unanswered(now time.Time) time.Duration {
	since := c.reading.Load()
	last, asked := c.lastRead.Load(), c.lastWrite.Load()
	if since == 0 || asked <= last+int64(ackGrace) || asked > last+int64(askedWithin) || c.run.Load() < bulkRun ||
		c.runRate.Load() < bulkRate || c.upSinceRead.Load() > smallRequest {
		return 0
	}
	return now.Sub(time.Unix(0, max(since, asked)))
}

func storeMax(v *atomic.Int64, x int64) {
	for {
		old := v.Load()
		if x <= old || v.CompareAndSwap(old, x) {
			return
		}
	}
}

// record files the flow record of a finished connection if anything about it is notable.
func (c *nodeConn) record() {
	if !c.watchStalls || c.group == "" {
		return
	}
	now := time.Now()
	rec := FlowRecord{
		Net: "tcp", Group: c.group, Site: c.site, Node: c.tag, OpenMs: c.openMs, TTFBMs: c.ttfbMs, Tried: c.tried,
		AgeMs: now.Sub(c.opened).Milliseconds(), Down: c.down.Load(), Up: c.up.Load(), Stalls: int(c.stallCount.Load()),
		ClientMs: c.clientMax.Load() / int64(time.Millisecond),
	}
	midWait := c.midWait.Load()
	if since := c.reading.Load(); since != 0 {
		last := c.lastRead.Load()
		if pending := now.UnixNano() - max(since, last); pending >= int64(time.Second) && c.midAnswer(last) {
			rec.EndedMid = true
			midWait = max(midWait, pending)
		}
	}
	rec.MidWaitMs = midWait / int64(time.Millisecond)
	rec.Close = "client"
	if why := c.why.Load(); why != nil {
		rec.Close = *why
	}
	if flowNotable(&rec) {
		c.health.flows.add(rec)
	}
}

// A stream stalls when it has been delivering data (stallStreak bytes in one run), goes
// silent for stallMin in the middle of an answer and then carries on. "Middle of an answer"
// means the client asked nothing new meanwhile; writes within ackGrace of the last data are
// acknowledgements (HTTP/2 window updates), not requests.
// "Carries on" means stallResume bytes more before the client asks anything new: a server
// closing an idle HTTP/2 connection sends one last small frame (GOAWAY, close_notify) after
// its idle timeout, and Instagram's CDN did that exactly 65s after every video segment.
const (
	stallMin    = 2 * time.Second
	stallStreak = 64 << 10
	stallResume = 16 << 10
	ackGrace    = 200 * time.Millisecond
	udpStallMin = 3 * time.Second
)

// pendingStall is a gap waiting for the stream to carry on before it counts.
type pendingStall struct {
	gap   time.Duration
	at    int64
	bytes int64
}

func newNodeConn(health *healthRegistry, tag string, conn net.Conn) *nodeConn {
	c := untrackedNodeConn(health, tag, conn)
	health.trackConn(tag, c)
	return c
}

// untrackedNodeConn is newNodeConn for a caller that sets the conn up before the node's
// sweeps can see it, then tracks it itself.
func untrackedNodeConn(health *healthRegistry, tag string, conn net.Conn) *nodeConn {
	c := &nodeConn{Conn: conn, health: health, tag: tag, stats: health.stats(tag), opened: time.Now()}
	c.lastRead.Store(time.Now().UnixNano())
	return c
}

// noteGap checks data that just arrived at now for a stall: waited is how long the read sat
// waiting for it, prev when the previous data came.
func (c *nodeConn) noteGap(now int64, waited time.Duration, prev int64, n int) {
	if waited >= stallMin && c.streak >= stallStreak && c.lastWrite.Load() < prev+int64(ackGrace) {
		c.stall = pendingStall{gap: waited, at: now}
	}
	if c.stall.gap > 0 {
		if c.lastWrite.Load() > c.stall.at+int64(ackGrace) {
			// a new request: what follows is its answer
			c.stall = pendingStall{}
		} else if c.stall.bytes += int64(n); c.stall.bytes >= stallResume {
			c.health.recordStall(c.tag, c.group, c.site, c.stall.gap, false)
			c.stats.real.streamStalls.add(1, realHalfLife)
			c.stallCount.Add(1)
			c.stall = pendingStall{}
		}
	}
	if waited >= time.Second {
		c.streak = 0
	}
	before := c.streak
	if before == 0 {
		c.runStart = now
	}
	c.streak += int64(n)
	if before < stallStreak && c.streak >= stallStreak {
		// a stream that got going: the chance a stall had to happen
		c.stats.real.streams.add(1, realHalfLife)
	}
	c.run.Store(c.streak)
	c.runRate.Store(c.streak * int64(time.Second) / max(now-c.runStart, int64(10*time.Millisecond)))
}

func (c *nodeConn) Read(p []byte) (int, error) {
	start := time.Now()
	if last := c.lastReturn.Load(); last != 0 {
		// between two reads the copier hands the data to the client: a slow client shows here
		storeMax(&c.clientMax, start.UnixNano()-last)
	}
	c.reading.Store(start.UnixNano())
	defer c.reading.Store(0)
	n, err := c.Conn.Read(p)
	now := time.Now()
	c.lastReturn.Store(now.UnixNano())
	if n > 0 {
		prev := c.lastRead.Swap(now.UnixNano())
		if waited := now.Sub(start); waited >= time.Second && c.midAnswer(prev) {
			storeMax(&c.midWait, int64(waited))
		}
		if c.watchStalls {
			c.noteGap(now.UnixNano(), now.Sub(start), prev, n)
		}
		c.stats.bytesDown.Add(int64(n))
		c.down.Add(int64(n))
		c.downSinceWrite.Add(int64(n))
		c.upSinceRead.Store(0)
		if c.tls != nil {
			c.tlsAccess.Lock()
			c.tls.feed(p[:n])
			c.tlsAccess.Unlock()
		}
	}
	if err != nil && !c.closing.Load() {
		c.setWhy("remote: " + shortErr(err))
		c.judge(false)
	}
	return n, err
}

func (c *nodeConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.stats.bytesUp.Add(int64(n))
	c.up.Add(int64(n))
	c.upSinceRead.Add(int64(n))
	if n > 0 {
		c.lastWrite.Store(time.Now().UnixNano())
		c.downSinceWrite.Store(0)
	}
	return n, err
}

// Limits for calling a connection unanswered. A TLS 1.3 server sends session tickets on
// its own right after the handshake, a few hundred bytes, so a real answer means more
// than that. When the client closes first, it must have waited a while: a closed tab is
// not a blocked site. When the far end closes, it must be right after the request: a
// geo-fence hangs up at once, while a server dropping an idle keep-alive after a tiny answer
// (a 301, a 204) takes its keep-alive timeout, 5s and up.
const (
	answerBytes       = 600
	answerWindow      = time.Minute
	clientGiveUp      = 8 * time.Second
	serverCloseWindow = 3 * time.Second
)

// judge reports a request that got no answer, once per connection.
// cutStalled reports a stream stuck inside a TLS record for at least idle.
func (c *nodeConn) cutStalled(idle time.Duration) bool {
	if c.tls == nil || c.silentFor() < idle {
		return false
	}
	c.tlsAccess.Lock()
	defer c.tlsAccess.Unlock()
	return c.tls.inside()
}

// reportCut fires onCut once per connection.
func (c *nodeConn) reportCut() bool {
	if c.onCut == nil || !c.cut.CompareAndSwap(false, true) {
		return false
	}
	c.onCut()
	return true
}

func (c *nodeConn) judge(byClient bool) {
	if c.debugf != nil {
		c.tlsAccess.Lock()
		valid, need, have := c.tls.valid, c.tls.need, c.tls.have
		c.tlsAccess.Unlock()
		c.debugf("judge byClient=", byClient, " idle=", c.idleFor(), " sinceWrite=", c.downSinceWrite.Load(), " tls valid=", valid, " need=", need, " have=", have)
	}
	if c.onCut != nil && c.cutStalled(cutIdle) {
		c.reportCut()
		return
	}
	if c.onUnanswered == nil || c.judged.Load() {
		return
	}
	last := c.lastWrite.Load()
	if last == 0 || c.downSinceWrite.Load() > answerBytes {
		return
	}
	waited := time.Since(time.Unix(0, last))
	if waited > answerWindow || byClient && waited < clientGiveUp || !byClient && waited > serverCloseWindow {
		return
	}
	if c.judged.CompareAndSwap(false, true) {
		c.onUnanswered()
	}
}

// awaitingAnswer: the client wrote after the last byte that came back.
func (c *nodeConn) awaitingAnswer() bool {
	return c.lastWrite.Load() > c.lastRead.Load()
}

func (c *nodeConn) idleFor() time.Duration {
	return time.Since(time.Unix(0, c.lastRead.Load()))
}

// silentFor is how long a pending read has been waiting on the far end; zero when no read
// is pending, see reading.
func (c *nodeConn) silentFor() time.Duration {
	since := c.reading.Load()
	if since == 0 {
		return 0
	}
	return time.Since(time.Unix(0, max(since, c.lastRead.Load())))
}

func (c *nodeConn) Close() error {
	c.closing.Store(true)
	c.once.Do(func() {
		c.health.untrackConn(c.tag, c)
		if !c.quiet.Load() {
			c.judge(true)
		}
		c.record()
	})
	return c.Conn.Close()
}

// closeForNode closes the conn because its node was judged dead or lossy. An idle
// keep-alive closed that way is not a site that left the client without an answer; judged
// like a client hang-up, every such conn after a tiny answer was a strike against its site.
func (c *nodeConn) closeForNode() error {
	c.quiet.Store(true)
	return c.Close()
}

// ---- UDP ----

func (g *smartGroup) newPacketConnection(ctx context.Context, cm adapter.ConnectionManager, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	g.touch()
	cm.NewPacketConnection(ctx, &groupPacketDialer{g: g}, conn, metadata, onClose)
}

type groupPacketDialer struct {
	g *smartGroup
}

func (d *groupPacketDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.g.dialSerial(ctx, network, destination)
}

func (d *groupPacketDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return newMigratingPacketConn(ctx, d.g, destination)
}

// dialSerial is the plain path used when something dials through the group directly
// (rule-set downloads, DNS detours, UDP connect): members in candidate order until one works.
func (g *smartGroup) dialSerial(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	g.touch()
	var lastErr error
	for i, out := range g.candidates(N.NetworkName(network)) {
		if i >= maxSerialAttempts {
			break
		}
		conn, err := out.DialContext(ctx, network, destination)
		if err == nil {
			return newNodeConn(g.health, out.Tag(), conn), nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = E.New("missing supported outbound")
	}
	return nil, lastErr
}

// A node gets this long to open a UDP session before the next one is tried. Over VLESS or
// Trojan that is a new TCP connection and handshake, which a dead node holds for its full dial
// timeout; the flow on its way to another node waited that long.
const udpOpenTimeout = 2 * time.Second

// listenWithin opens a UDP session through out, giving up on it after timeout. The attempt is
// not cancelled, only left behind: a session that comes up late is closed. The context is not
// tied to the timeout since some outbounds keep the session bound to the one they were given.
func listenWithin(ctx context.Context, out adapter.Outbound, destination M.Socksaddr, timeout time.Duration) (net.PacketConn, error) {
	type opened struct {
		pc  net.PacketConn
		err error
	}
	result := make(chan opened, 1)
	go func() {
		pc, err := out.ListenPacket(ctx, destination)
		result <- opened{pc, err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-result:
		return r.pc, r.err
	case <-timer.C:
		go func() {
			if r := <-result; r.pc != nil {
				r.pc.Close()
			}
		}()
		return nil, E.New("no UDP session through ", out.Tag(), " within ", timeout)
	}
}

func (g *smartGroup) listenSerial(ctx context.Context, destination M.Socksaddr, exclude string) (net.PacketConn, adapter.Outbound, error) {
	var lastErr error
	tried := 0
	for _, out := range g.candidates(N.NetworkUDP) {
		if out.Tag() == exclude {
			continue
		}
		if tried >= maxSerialAttempts {
			break
		}
		tried++
		pc, err := listenWithin(ctx, out, destination, udpOpenTimeout)
		if err == nil {
			return pc, out, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = E.New("missing supported outbound")
	}
	return nil, nil, lastErr
}
