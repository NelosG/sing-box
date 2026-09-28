package group

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type fakeOutbound struct {
	outbound.Adapter
}

func (f *fakeOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, E.New("fake")
}

func (f *fakeOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, E.New("fake")
}

type fakeManager struct {
	adapter.OutboundManager
	byTag map[string]adapter.Outbound
}

func (m *fakeManager) Outbound(tag string) (adapter.Outbound, bool) {
	out, ok := m.byTag[tag]
	return out, ok
}

const testLink = "https://probe.test/"

func newTestGroup(t *testing.T, tags []string, o smartOptions) (*smartGroup, *fakeManager) {
	t.Helper()
	return newTypedTestGroup(t, tags, nil, o)
}

// newTypedTestGroup is newTestGroup with some members of a given outbound type (a local
// exit such as zapret's socks); the rest are proxy nodes.
func newTypedTestGroup(t *testing.T, tags []string, types map[string]string, o smartOptions) (*smartGroup, *fakeManager) {
	t.Helper()
	manager := &fakeManager{byTag: map[string]adapter.Outbound{}}
	for _, tag := range tags {
		typ := types[tag]
		if typ == "" {
			typ = "fake"
		}
		manager.byTag[tag] = &fakeOutbound{outbound.NewAdapter(typ, tag, []string{N.NetworkTCP, N.NetworkUDP}, nil)}
	}
	o.link = testLink
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, tags, o)
	t.Cleanup(func() { g.Close() })
	// most tests are about a group past its first probe round; cold-start tests reset this
	g.headProbed.Store(true)
	g.settled.Store(true)
	// a confirming re-measurement sees what the test last recorded, and runs inline
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		g.health.access.Lock()
		defer g.health.access.Unlock()
		if p := g.health.probes[probeKey{out.Tag(), testLink}]; p != nil {
			return p.last, p.status, nil
		}
		return 0, 0, E.New("no answer")
	}
	g.async = func(f func()) { f() }
	return g, manager
}

func alive(g *smartGroup, tag string, rtt uint16) {
	g.health.Success(tag, testLink, rtt, 204)
}

func kill(g *smartGroup, tag string) {
	for i := 0; i < deadAfterFailures; i++ {
		g.health.Failure(tag, deadAfterFailures)
	}
}

func TestLatencyModeKeepsPrimaryWithinTolerance(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 200)
	alive(g, "b", 150)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("list order must win ties within tolerance, got %s", g.Now())
	}
	alive(g, "b", 10)
	alive(g, "b", 10)
	alive(g, "b", 10)
	alive(g, "b", 10)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("clearly faster node must take over, got %s", g.Now())
	}
}

func TestDeadPrimaryIsReplacedAtOnce(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 50})
	alive(g, "a", 50)
	alive(g, "b", 60)
	g.reselect()
	kill(g, "a")
	if g.Now() != "b" {
		t.Fatalf("expected failover to b on death, got %s", g.Now())
	}
	if g.health.State("a") != stateDead {
		t.Fatalf("a should be dead after %d failures", deadAfterFailures)
	}
}

func TestSuspectIsNotDeadAndRecovers(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a"}, smartOptions{})
	alive(g, "a", 50)
	if g.health.Failure("a", deadAfterFailures) != stateSuspect {
		t.Fatal("one failure must only make a node suspect")
	}
	alive(g, "a", 50)
	if g.health.State("a") != stateAlive {
		t.Fatal("a success must clear suspicion")
	}
}

func TestFasterWithinToleranceWinsOnSpeed(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b", "c"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 120)
	alive(g, "c", 400)
	g.health.setSpeedSample("a", 2_000_000)
	g.health.setSpeedSample("b", 20_000_000)
	g.health.setSpeedSample("c", 90_000_000)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("b is within tolerance and 10x faster, got %s", g.Now())
	}
	g.health.setSpeedSample("a", 18_000_000)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("a is not faster by the margin, b must stay, got %s", g.Now())
	}
}

func TestFallbackLocalHeadWinsAndWaitsAfterDeath(t *testing.T) {
	g, _ := newTypedTestGroup(t, []string{"zapret", "first", "fast"}, map[string]string{"zapret": C.TypeSOCKS}, smartOptions{mode: "fallback"})
	alive(g, "fast", 5)
	g.reselect()
	alive(g, "zapret", 500)
	g.reselect()
	if g.Now() != "zapret" {
		t.Fatalf("a local exit at the head never died, it must win at once, got %s", g.Now())
	}
	alive(g, "first", 400)
	kill(g, "zapret")
	if g.Now() != "fast" {
		t.Fatalf("with the head down the best proxy takes over, not the next in the list, got %s", g.Now())
	}
	alive(g, "zapret", 500)
	g.reselect()
	if g.Now() != "fast" {
		t.Fatalf("the head came back from the dead only now, it must prove itself first, got %s", g.Now())
	}
	revivedAgo := func(d time.Duration) {
		g.health.access.Lock()
		g.health.nodes["zapret"].revivedAt = time.Now().Add(-d)
		g.health.access.Unlock()
	}
	revivedAgo(headQuickAfter + time.Second)
	probeSeq(g, "zapret", repeat(500, headQuickProbes-1)...)
	g.reselect()
	if g.Now() != "fast" {
		t.Fatalf("%d probe is not enough yet, got %s", headQuickProbes-1, g.Now())
	}
	probeSeq(g, "zapret", 500)
	g.reselect()
	if g.Now() != "zapret" {
		t.Fatalf("after %d good probes a head that died once gets the group back, got %s", headQuickProbes, g.Now())
	}
	// a head that keeps dying proves itself longer
	kill(g, "zapret")
	alive(g, "zapret", 500)
	revivedAgo(headQuickAfter + time.Second)
	probeSeq(g, "zapret", repeat(500, headQuickProbes)...)
	g.reselect()
	if g.Now() != "fast" {
		t.Fatalf("the head died twice lately, two probes must not do, got %s", g.Now())
	}
	revivedAgo(headReturnAfter + time.Second)
	probeSeq(g, "zapret", repeat(500, headReturnProbes-headQuickProbes)...)
	g.reselect()
	if g.Now() != "zapret" {
		t.Fatalf("after %d good probes even a blinking head gets the group back, got %s", headReturnProbes, g.Now())
	}
}

func TestRevivedNodeStartsWithACleanRecord(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{})
	probeSeq(g, "a", repeat(100, 8)...)
	kill(g, "a")
	// the retries while it is down fail, and must not count once it is back
	for i := 0; i < 4; i++ {
		g.health.recordProbe("a", false, false)
	}
	alive(g, "a", 100)
	if g.health.degraded("a") {
		t.Fatal("failures from before the revival kept the node out as lossy")
	}
	if since, streak := g.health.revival("a"); since <= 0 || streak != 0 {
		t.Fatalf("the revival starts a new streak: %v %d", since, streak)
	}
}

func TestOneDeathIsFreeRepeatedOnesCost(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a"}, smartOptions{})
	probeSeq(g, "a", repeat(100, 8)...)
	kill(g, "a")
	probeSeq(g, "a", repeat(100, 8)...)
	one, _ := g.fitAs(manager(g, "a"), false)
	if one > 150 {
		t.Fatalf("a node that blinked once and is back is as good as before: %v", one)
	}
	kill(g, "a")
	probeSeq(g, "a", repeat(100, 8)...)
	two, _ := g.fitAs(manager(g, "a"), false)
	if two < one+500 {
		t.Fatalf("a node that keeps dying must pay for it: %v after one death, %v after two", one, two)
	}
}

func TestFallbackDriftsToABetterNodeAfterAWhile(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{mode: "fallback", tolerance: 100})
	probeSeq(g, "a", repeat(300, 8)...)
	g.reselect()
	probeSeq(g, "b", repeat(100, 8)...)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("better by the tolerance only just now: the group stays, got %s", g.Now())
	}
	g.access.Lock()
	g.betterSince = time.Now().Add(-fallbackDrift - time.Second)
	g.access.Unlock()
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("better for %v: the group moves, got %s", fallbackDrift, g.Now())
	}
	probeSeq(g, "a", repeat(50, 16)...)
	g.access.Lock()
	g.betterSince = time.Now().Add(-fallbackDrift - time.Second)
	g.access.Unlock()
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("at most one such move per %v, got %s", fallbackDriftEvery, g.Now())
	}
}

func TestFallbackAmongProxiesGoesByScoreAndStays(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b", "c"}, smartOptions{mode: "fallback", tolerance: 100})
	g.settled.Store(false)
	probeSeq(g, "a", repeat(400, 8)...)
	probeSeq(g, "b", repeat(100, 8)...)
	probeSeq(g, "c", repeat(150, 8)...)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("during the first probe round the group holds its first answer, got %s", g.Now())
	}
	g.finalPick.Store(true)
	g.reselect()
	g.finalPick.Store(false)
	if g.Now() != "b" {
		t.Fatalf("proxies stand in subscription order, the best score must win, got %s", g.Now())
	}
	g.settled.Store(true)
	probeSeq(g, "c", repeat(60, 16)...)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("a fallback group keeps one steady exit while it works, got %s", g.Now())
	}
	probeSeq(g, "b", repeat(1500, 16)...)
	g.reselect()
	if g.Now() != "c" {
		t.Fatalf("a primary far worse than the best must be left, got %s", g.Now())
	}
	kill(g, "c")
	if g.Now() != "b" && g.Now() != "a" {
		t.Fatalf("a dead primary is replaced, got %s", g.Now())
	}
	if cands := g.candidates(N.NetworkTCP); cands[0].Tag() != g.Now() {
		t.Fatalf("the primary leads the race, got %s", cands[0].Tag())
	}
}

func TestMinSpeedJudgesOnlyTestedProxies(t *testing.T) {
	g, _ := newTypedTestGroup(t, []string{"zapret", "a"}, map[string]string{"zapret": C.TypeSOCKS}, smartOptions{mode: "fallback", minSpeed: 15})
	alive(g, "zapret", 50)
	alive(g, "a", 50)
	// what a quiet minute of real traffic or the stamp before a test leaves behind
	g.health.setSpeedSample("zapret", 100_000)
	g.health.setSpeedSample("a", 100_000)
	if _, ok := g.fit(manager(g, "zapret")); !ok {
		t.Fatal("a local exit is never held to min_speed")
	}
	if _, ok := g.fit(manager(g, "a")); !ok {
		t.Fatal("without a finished test a proxy is not known to be slow")
	}
	g.health.setTestedSpeed("a", 100_000)
	if _, ok := g.fit(manager(g, "a")); ok {
		t.Fatal("a proxy that tested below min_speed is unfit")
	}
}

func TestFallbackRaceOrderKeepsLocalExitsFirst(t *testing.T) {
	g, _ := newTypedTestGroup(t, []string{"zapret", "slow", "quick"}, map[string]string{"zapret": C.TypeSOCKS}, smartOptions{mode: "fallback"})
	alive(g, "zapret", 300)
	alive(g, "slow", 400)
	alive(g, "quick", 50)
	g.reselect()
	var got []string
	for _, out := range g.candidates(N.NetworkTCP) {
		got = append(got, out.Tag())
	}
	if len(got) != 3 || got[0] != "zapret" || got[1] != "quick" || got[2] != "slow" {
		t.Fatalf("the local head leads, the proxies back it up best first, got %v", got)
	}
}

func TestSiteMemoryOnlyInFallbackMode(t *testing.T) {
	g, m := newTestGroup(t, []string{"direct", "proxy"}, smartOptions{mode: "fallback", siteTTL: time.Hour})
	alive(g, "direct", 1)
	alive(g, "proxy", 30)
	g.reselect()
	direct, proxy := m.byTag["direct"], m.byTag["proxy"]
	g.remember("blocked.example", direct, proxy)
	cands, _ := g.preferRemembered("blocked.example", g.candidates(N.NetworkTCP))
	if cands[0] != proxy {
		t.Fatalf("remembered exit must come first, got %s", cands[0].Tag())
	}
	if other, _ := g.preferRemembered("fine.example", g.candidates(N.NetworkTCP)); other[0] != direct {
		t.Fatal("other sites must keep the list order")
	}
	g.sites.Store("old.example", siteChoice{tag: "proxy", until: time.Now().Add(-time.Second)})
	if expired, _ := g.preferRemembered("old.example", g.candidates(N.NetworkTCP)); expired[0] != direct {
		t.Fatal("expired memory must give direct another chance")
	}

	lat, lm := newTestGroup(t, []string{"a", "b"}, smartOptions{})
	alive(lat, "a", 1)
	alive(lat, "b", 1)
	lat.reselect()
	lat.remember("x.example", lm.byTag["a"], lm.byTag["b"])
	if _, found := lat.sites.Load("x.example"); found {
		t.Fatal("latency mode must not pin sites to a node that won one race")
	}
}

func TestExcludedEgressNeverCandidate(t *testing.T) {
	g, _ := newTestGroup(t, []string{"gb", "nl"}, smartOptions{exclude: []string{"GB"}})
	alive(g, "gb", 1)
	alive(g, "nl", 300)
	g.health.setCountry("gb", "GB")
	g.health.setCountry("nl", "NL")
	g.reselect()
	if g.Now() != "nl" {
		t.Fatalf("GB exit must not be primary, got %s", g.Now())
	}
	for _, out := range g.candidates(N.NetworkTCP) {
		if out.Tag() == "gb" {
			t.Fatal("GB exit must not even be a race backup")
		}
	}
}

func TestDeadRetryBacksOff(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a"}, smartOptions{})
	kill(g, "a")
	finish, _ := g.health.beginProbe(probeKey{"a", testLink})
	finish()
	if g.health.deadRetryDue("a", time.Hour) {
		t.Fatal("just probed, retry must not be due")
	}
	g.health.access.Lock()
	g.health.nodes["a"].lastAttempt = time.Now().Add(-11 * time.Minute)
	g.health.nodes["a"].deadFails = 50
	g.health.access.Unlock()
	if !g.health.deadRetryDue("a", 15*time.Second) {
		t.Fatalf("backoff must cap at %v", maxDeadRetry)
	}
}

func TestMemberReplacementResetsHealth(t *testing.T) {
	g, m := newTestGroup(t, []string{"a"}, smartOptions{})
	g.members()
	kill(g, "a")
	m.byTag["a"] = &fakeOutbound{outbound.NewAdapter("fake", "a", []string{N.NetworkTCP}, nil)}
	g.members()
	if g.health.State("a") != stateUnknown {
		t.Fatalf("new credentials start from a clean record, got %s", g.health.State("a"))
	}
}

func TestEventLogRingNewestFirst(t *testing.T) {
	var l eventLog
	for i := 0; i < maxEvents+10; i++ {
		l.add(healthEvent{Kind: "k", Detail: string(rune('a' + i%26))})
	}
	list := l.list()
	if len(list) != maxEvents {
		t.Fatalf("ring must hold %d, got %d", maxEvents, len(list))
	}
	if list[0].Detail != string(rune('a'+(maxEvents+9)%26)) {
		t.Fatal("newest event must come first")
	}
}

func TestIsTLSClientHello(t *testing.T) {
	if !isTLSClientHello([]byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00}) {
		t.Fatal("client hello not recognised")
	}
	if isTLSClientHello([]byte("GET / HTTP/1.1\r\n")) {
		t.Fatal("plain HTTP must never be raced")
	}
	if isTLSClientHello([]byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x02}) {
		t.Fatal("server hello is not a client hello")
	}
}

func TestSiteKeyPrefersSniffedDomain(t *testing.T) {
	md := adapter.InboundContext{Domain: "a.example", Destination: M.ParseSocksaddrHostPort("1.2.3.4", 443)}
	if siteKey(md) != "a.example" {
		t.Fatal("sniffed domain must name the site")
	}
	md = adapter.InboundContext{Destination: M.ParseSocksaddrHostPort("b.example", 443)}
	if siteKey(md) != "b.example" {
		t.Fatal("fqdn destination must name the site")
	}
}

func TestSpeedTieBreakLeavesDirectAlone(t *testing.T) {
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"direct": &fakeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP}, nil)},
		"node":   &fakeOutbound{outbound.NewAdapter(C.TypeVLESS, "node", []string{N.NetworkTCP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, []string{"direct", "node"}, smartOptions{link: testLink, tolerance: 300})
	t.Cleanup(func() { g.Close() })
	alive(g, "direct", 20)
	alive(g, "node", 150)
	g.health.setSpeedSample("node", 50_000_000)
	g.reselect()
	if g.Now() != "direct" {
		t.Fatalf("a measured proxy must not push a direct-first group off direct, got %s", g.Now())
	}
}

func TestSpeedTieBreakNeedsKnownSpeeds(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 60)
	g.health.setSpeedSample("b", 50_000_000)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("unknown speed of the latency winner is no reason to leave it, got %s", g.Now())
	}
}

func TestSuspectPrimaryStaysUntilDead(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 50})
	alive(g, "a", 50)
	alive(g, "b", 60)
	g.reselect()
	g.health.Failure("a", deadAfterFailures)
	if g.Now() != "a" {
		t.Fatalf("one failed probe must not move the primary, got %s", g.Now())
	}
	g.health.Failure("a", deadAfterFailures)
	g.health.Failure("a", deadAfterFailures)
	if g.Now() != "b" {
		t.Fatalf("a dead primary must be replaced, got %s", g.Now())
	}
}

func TestOptimisingSwitchesAreRateLimited(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b", "c"}, smartOptions{tolerance: 10})
	settle := func(tag string, rtt uint16) {
		for i := 0; i < 20; i++ {
			alive(g, tag, rtt)
		}
	}
	settle("a", 100)
	settle("b", 200)
	settle("c", 300)
	g.reselect()
	g.settled.Store(false)
	settle("c", 250)
	g.reselect()
	g.settled.Store(true)
	if g.Now() != "a" {
		t.Fatalf("before the first probe round the list order and delay decide, got %s", g.Now())
	}
	settle("b", 50)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("first optimisation after settling must happen, got %s", g.Now())
	}
	settle("c", 5)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("a second optimisation within %v must wait, got %s", optimizeInterval, g.Now())
	}
	kill(g, "b")
	if g.Now() != "c" {
		t.Fatalf("death must switch at once regardless of the rate limit, got %s", g.Now())
	}
}

func TestBadPrimaryEscapesTheRateLimit(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 50})
	fresh := map[string]uint16{"a": 200, "b": 100}
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return fresh[out.Tag()], 204, nil
	}
	alive(g, "a", 200)
	g.reselect()
	// an ordinary optimisation uses up the rate limit
	alive(g, "b", 100)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("setup: expected an optimisation to b, got %s", g.Now())
	}
	moved := g.Now()
	// the new primary starts losing packets: probes still pass, but take seconds
	fresh[moved] = 2500
	for i := 0; i < 6; i++ {
		alive(g, moved, 2500)
	}
	g.reselect()
	if g.Now() == moved {
		t.Fatalf("a primary far slower than the best must not wait %v, still on %s", optimizeInterval, moved)
	}
}

func TestOneSlowProbeIsNoEscape(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return map[string]uint16{"a": 120, "b": 90}[out.Tag()], 204, nil
	}
	alive(g, "a", 120)
	alive(g, "b", 90)
	g.reselect()
	// one probe of the primary landed in a 3s stall
	alive(g, "a", 3000)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("measured again the primary is fine, it must stay, got %s", g.Now())
	}
}

func TestUnsettledGroupMovesToBestAtOnce(t *testing.T) {
	g, _ := newTestGroup(t, []string{"zapret", "far"}, smartOptions{tolerance: 100})
	g.settled.Store(false)
	alive(g, "far", 300)
	g.reselect()
	if g.Now() != "far" {
		t.Fatalf("the first answer becomes primary, got %s", g.Now())
	}
	alive(g, "zapret", 20)
	g.reselect()
	if g.Now() != "zapret" {
		t.Fatalf("right after start the real best must win without waiting, got %s", g.Now())
	}
}

func TestSiteLogicOnlyForDirectFirstGroups(t *testing.T) {
	nodes, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{mode: "fallback"})
	if nodes.siteAware() {
		t.Fatal("a fallback group of proxy nodes must keep one steady exit, not move sites")
	}
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"direct": &fakeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP}, nil)},
		"proxy":  &fakeOutbound{outbound.NewAdapter(C.TypeURLTest, "proxy", []string{N.NetworkTCP}, nil)},
	}}
	auto := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "auto", urltest.NewHistoryStorage(), manager, []string{"direct", "proxy"}, smartOptions{link: testLink, mode: "fallback"})
	t.Cleanup(func() { auto.Close() })
	if !auto.siteAware() {
		t.Fatal("direct-first fallback group must move blocked sites")
	}
	explicit, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{mode: "fallback", siteTTL: time.Hour})
	if !explicit.siteAware() {
		t.Fatal("explicit site_memory turns site logic on")
	}
}

func TestColdStartReturnsToListHead(t *testing.T) {
	g, _ := newTestGroup(t, []string{"direct", "far"}, smartOptions{tolerance: 300})
	g.settled.Store(false)
	alive(g, "far", 80)
	g.reselect()
	if g.Now() != "far" {
		t.Fatalf("the first to answer leads for now, got %s", g.Now())
	}
	alive(g, "direct", 16)
	g.reselect()
	if g.Now() != "direct" {
		t.Fatalf("before settling the list head must win within tolerance, got %s", g.Now())
	}
}

func TestBurstSampleDoesNotMoveGroupOffListHead(t *testing.T) {
	for _, settled := range []bool{false, true} {
		g, _ := newTestGroup(t, []string{"head", "other"}, smartOptions{tolerance: 150})
		g.settled.Store(settled)
		fresh := map[string]uint16{"head": 130, "other": 95}
		g.measure = func(out adapter.Outbound) (uint16, int, error) {
			return fresh[out.Tag()], 204, nil
		}
		alive(g, "head", 120)
		g.reselect()
		// one probe of the head landed in the start-up burst
		alive(g, "head", 871)
		alive(g, "other", 91)
		g.reselect()
		if g.Now() != "head" {
			t.Fatalf("settled=%v: head is within tolerance and must stay, got %s", settled, g.Now())
		}
		if !settled {
			// the first probe round ends; now the stored gap is looked at, and measured again
			g.settled.Store(true)
			g.reselect()
			if g.Now() != "head" {
				t.Fatalf("after settling, measured again, head must stay, got %s", g.Now())
			}
		}
		if rtt := g.health.RTT("head", testLink); rtt != 130 {
			t.Fatalf("settled=%v: the fresh measurement must replace the inflated delay, got %v", settled, rtt)
		}
	}
}

func TestUnsettledGroupDoesNotMoveDownOnDelay(t *testing.T) {
	g, _ := newTestGroup(t, []string{"head", "fast"}, smartOptions{tolerance: 100})
	g.settled.Store(false)
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return map[string]uint16{"head": 600, "fast": 10}[out.Tag()], 204, nil
	}
	alive(g, "head", 600)
	g.reselect()
	alive(g, "fast", 10)
	g.reselect()
	if g.Now() != "head" {
		t.Fatalf("a working head must hold until the first probe round is over, got %s", g.Now())
	}
	g.settled.Store(true)
	g.reselect()
	if g.Now() != "fast" {
		t.Fatalf("after the round a confirmed gap moves the group, got %s", g.Now())
	}
}

func TestConfirmedDelayGapStillSwitches(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 50})
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		if out.Tag() == "a" {
			return 300, 204, nil
		}
		return 20, 204, nil
	}
	alive(g, "a", 100)
	g.reselect()
	alive(g, "b", 20)
	g.reselect()
	if g.Now() != "b" {
		t.Fatalf("the gap held up on a fresh measurement, b must take over, got %s", g.Now())
	}
}

func TestListHeadHoldsUntilItsOwnProbesReturn(t *testing.T) {
	for _, mode := range []string{"latency", "fallback"} {
		g, _ := newTestGroup(t, []string{"head", "next", "last"}, smartOptions{tolerance: 100, mode: mode})
		g.settled.Store(false)
		g.headProbed.Store(false)
		// another group sharing the registry got an answer from "next" first
		alive(g, "next", 40)
		if g.Now() != "head" {
			t.Fatalf("%s: before its own probes the group must hold the list head, got %s", mode, g.Now())
		}
		kill(g, "head")
		if g.Now() != "next" {
			t.Fatalf("%s: a head known dead must not be held, got %s", mode, g.Now())
		}
		alive(g, "head", 50)
		g.headProbed.Store(true)
		g.reselect()
		// latency mode takes the head back at once: one death is free and before the first
		// round the list order decides; a fallback group holds what it has until the round ends
		want := "head"
		if mode == "fallback" {
			want = "next"
		}
		if g.Now() != want {
			t.Fatalf("%s: after the head probes, expected %s, got %s", mode, want, g.Now())
		}
	}
}

func TestFallbackSuspectPrimaryStays(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{mode: "fallback"})
	alive(g, "a", 100)
	alive(g, "b", 100)
	g.reselect()
	g.health.Failure("a", deadAfterFailures)
	if g.Now() != "a" {
		t.Fatalf("one lost probe must not move a fallback group, got %s", g.Now())
	}
	alive(g, "a", 100)
	g.health.Failure("a", deadAfterFailures)
	g.health.Failure("a", deadAfterFailures)
	g.health.Failure("a", deadAfterFailures)
	if g.Now() != "b" {
		t.Fatalf("a dead primary must be replaced, got %s", g.Now())
	}
}

// pipeOutbound answers every dial with one end of a pipe.
type pipeOutbound struct {
	outbound.Adapter
}

func (p *pipeOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	client, server := net.Pipe()
	server.Close()
	return client, nil
}

func (p *pipeOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, E.New("pipe")
}

func TestBlockedSiteDoesNotFailDirect(t *testing.T) {
	for _, first := range []string{C.TypeDirect, "vless"} {
		manager := &fakeManager{byTag: map[string]adapter.Outbound{
			"first": &fakeOutbound{outbound.NewAdapter(first, "first", []string{N.NetworkTCP}, nil)},
			"node":  &pipeOutbound{outbound.NewAdapter("vless", "node", []string{N.NetworkTCP}, nil)},
		}}
		g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, []string{"first", "node"}, smartOptions{link: testLink, tolerance: 100})
		g.headProbed.Store(true)
		g.settled.Store(true)
		alive(g, "first", 10)
		alive(g, "node", 50)
		g.reselect()
		// The lost race sends the first choice to a re-check, whose probes the fake exit fails
		// in the background; this test is about the race's own verdict, so that check counts as
		// already running.
		g.verifying.Store("first", struct{}{})
		conn, err := g.dialRace(context.Background(), adapter.InboundContext{Destination: M.ParseSocksaddr("blocked.example:443")}, nil, true, false)
		if err != nil {
			t.Fatalf("%s: the node should have carried the connection: %v", first, err)
		}
		conn.Close()
		state := g.health.State("first")
		if first == C.TypeDirect && state != stateAlive {
			t.Fatalf("one site refusing direct must not count against direct, state %v", state)
		}
		if first != C.TypeDirect && state != stateSuspect {
			t.Fatalf("a proxy node that failed where another got through is suspect, state %v", state)
		}
		g.Close()
	}
}

// watchedConn wraps one end of a pipe in a nodeConn that counts "unanswered" verdicts; the
// other end plays the site.
func watchedConn(t *testing.T) (*nodeConn, net.Conn, *atomic.Int32) {
	t.Helper()
	g, _ := newTestGroup(t, []string{"direct"}, smartOptions{})
	relaySide, site := net.Pipe()
	c := newNodeConn(g.health, "direct", relaySide)
	var verdicts atomic.Int32
	c.onUnanswered = func() { verdicts.Add(1) }
	t.Cleanup(func() { c.Close(); site.Close() })
	return c, site, &verdicts
}

func TestClientGoodbyeIsNotUnanswered(t *testing.T) {
	c, site, verdicts := watchedConn(t)
	go io.Copy(io.Discard, site)
	c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	go site.Write(make([]byte, 4000))
	io.ReadFull(c, make([]byte, 4000))
	// the client says goodbye and leaves; the relay closes while its read is still pending
	c.Write([]byte("close_notify"))
	readDone := make(chan struct{})
	go func() {
		c.Read(make([]byte, 16))
		close(readDone)
	}()
	c.Close()
	<-readDone
	if verdicts.Load() != 0 {
		t.Fatal("our own close after the client's goodbye must not count as a site that did not answer")
	}
}

func TestGeoFenceCloseRightAfterRequest(t *testing.T) {
	c, site, verdicts := watchedConn(t)
	go func() {
		site.Read(make([]byte, 64))
		site.Close()
	}()
	c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	c.Read(make([]byte, 16))
	if verdicts.Load() != 1 {
		t.Fatal("a site hanging up right after the request did not answer")
	}
}

func TestIdleKeepAliveCloseAfterTinyAnswer(t *testing.T) {
	c, site, verdicts := watchedConn(t)
	go io.Copy(io.Discard, site)
	c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	go site.Write(make([]byte, 300))
	io.ReadFull(c, make([]byte, 300))
	// the server's keep-alive timeout runs out later than serverCloseWindow
	c.lastWrite.Store(time.Now().Add(-serverCloseWindow - 2*time.Second).UnixNano())
	site.Close()
	c.Read(make([]byte, 16))
	if verdicts.Load() != 0 {
		t.Fatal("an idle keep-alive closed after a 301 is not a blocked site")
	}
}

// deadEndOutbound hands out UDP sockets on which every write fails, like a node whose
// remote end refuses one particular peer.
type deadEndOutbound struct {
	outbound.Adapter
}

func (o *deadEndOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, E.New("tcp not used here")
}

func (o *deadEndOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	pc.Close()
	return pc, nil
}

func TestFailingPeerDoesNotStormMigrations(t *testing.T) {
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"a": &deadEndOutbound{outbound.NewAdapter("vless", "a", []string{N.NetworkUDP}, nil)},
		"b": &deadEndOutbound{outbound.NewAdapter("vless", "b", []string{N.NetworkUDP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, []string{"a", "b"}, smartOptions{link: testLink, tolerance: 100})
	t.Cleanup(func() { g.Close() })
	g.headProbed.Store(true)
	g.settled.Store(true)
	for _, tag := range []string{"a", "b"} {
		alive(g, tag, 50)
		// both nodes were just probed and are fine; the failures below are the peer's
		g.health.access.Lock()
		g.health.node(tag).lastAttempt = time.Now().Add(time.Hour)
		g.health.access.Unlock()
	}
	g.reselect()
	peer := M.ParseSocksaddr("77.51.143.17:56421")
	// the app opens a new flow after each failure, as a browser's WebRTC stack does
	for i := 0; i < 4; i++ {
		pc, err := newMigratingPacketConn(context.Background(), g, peer)
		if err != nil {
			t.Fatal(err)
		}
		flow := pc.(*migratingPacketConn)
		for j := 0; j < 3; j++ {
			flow.WritePacket(buf.As([]byte("stun")), peer)
		}
		flow.Close()
	}
	moves := 0
	for _, e := range g.health.events.list() {
		if e.Kind == "udp_moved" {
			moves++
		}
	}
	if moves != 1 {
		t.Fatalf("a peer failing on every node is worth one move, not a storm; got %d moves", moves)
	}
}

func TestNodeVerdictCloseIsNotASiteStrike(t *testing.T) {
	for _, forNode := range []bool{true, false} {
		c, site, verdicts := watchedConn(t)
		go io.Copy(io.Discard, site)
		c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
		go site.Write(make([]byte, 200))
		io.ReadFull(c, make([]byte, 200))
		// an idle keep-alive after a tiny answer, well past clientGiveUp
		c.lastWrite.Store(time.Now().Add(-clientGiveUp - 2*time.Second).UnixNano())
		if forNode {
			c.closeForNode()
		} else {
			c.Close()
		}
		want := int32(0)
		if !forNode {
			// the same close coming from the client is how "the client gave up" is seen
			want = 1
		}
		if verdicts.Load() != want {
			t.Fatalf("forNode=%v: expected %d verdicts, got %d", forNode, want, verdicts.Load())
		}
	}
}

func TestCutSiteKeepsTheFirstChoiceOutOfTheRace(t *testing.T) {
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"direct": &fakeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP}, nil)},
		"proxy":  &fakeOutbound{outbound.NewAdapter(C.TypeURLTest, "proxy", []string{N.NetworkTCP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "auto", urltest.NewHistoryStorage(), manager, []string{"direct", "proxy"}, smartOptions{link: testLink, mode: "fallback"})
	t.Cleanup(func() { g.Close() })
	direct, proxy := manager.byTag["direct"], manager.byTag["proxy"]
	cands := []adapter.Outbound{direct, proxy}
	g.siteCut("cc.cdn.civiccomputing.com", direct)
	got, strict := g.preferRemembered("cc.cdn.civiccomputing.com", cands)
	if !strict || len(got) != 2 || got[0] != proxy || got[1] != direct {
		t.Fatalf("a site cut on direct keeps direct only as the last resort, got %v strict=%v", got, strict)
	}
	// a plain race loss is not evidence against direct: it stays in the race as a backup
	g.remember("slow.example", direct, proxy)
	got, strict = g.preferRemembered("slow.example", cands)
	if strict || len(got) != 2 || got[0] != proxy {
		t.Fatalf("a site moved by a lost race keeps direct as backup, got %v", got)
	}
}

func TestStrictSiteFallsBackWhenTheOtherExitCannotReachIt(t *testing.T) {
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"direct": &pipeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP}, nil)},
		// the proxy group cannot reach the host at all (a Russian site refusing foreign IPs)
		"proxy": &fakeOutbound{outbound.NewAdapter(C.TypeURLTest, "proxy", []string{N.NetworkTCP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "auto", urltest.NewHistoryStorage(), manager, []string{"direct", "proxy"}, smartOptions{link: testLink, mode: "fallback"})
	t.Cleanup(func() { g.Close() })
	g.headProbed.Store(true)
	g.settled.Store(true)
	alive(g, "direct", 10)
	alive(g, "proxy", 50)
	g.reselect()
	site := "95.213.154.113"
	// moved there earlier by mistake
	g.sites.Store(site, siteChoice{tag: "proxy", until: time.Now().Add(time.Hour), strict: true})
	conn, err := g.dialRace(context.Background(), adapter.InboundContext{Destination: M.ParseSocksaddr(site + ":443")}, nil, true, false)
	if err != nil {
		t.Fatalf("direct reaches the host, the connection must not fail: %v", err)
	}
	conn.Close()
	if !g.pinned(site) {
		t.Fatal("the mistaken move must be undone and the site pinned to direct")
	}
}

func TestRussianZones(t *testing.T) {
	for site, want := range map[string]bool{
		"api.ozon.ru": true, "m-api.a.mts.ru": true, "xn--d1acpjx3f.xn--p1ai": true, "mail.su": true,
		"www.msi.com": false, "ru.wikipedia.org": false, "rutracker.org": false, "nru": false,
	} {
		if russianSite(site) != want {
			t.Errorf("russianSite(%q) = %v", site, !want)
		}
	}
}

func TestOnlyFlowsWaitingForAnAnswerAreStalled(t *testing.T) {
	c := &migratingPacketConn{}
	now := time.Now()
	c.lastRx.Store(now.Add(-10 * time.Second).UnixNano())
	c.lastTx.Store(now.Add(-9 * time.Second).UnixNano())
	c.waitFrom.Store(now.Add(-9 * time.Second).UnixNano())
	if c.stalled(3 * time.Second) {
		t.Fatal("a flow silent both ways (paused video) is not stalled")
	}
	c.lastTx.Store(now.Add(-100 * time.Millisecond).UnixNano())
	if c.stalled(3 * time.Second) {
		t.Fatal("a peer that never answered (a dead WebRTC candidate) is not a broken path")
	}
	c.answered.Store(true)
	if !c.stalled(3 * time.Second) {
		t.Fatal("the app still sends and nothing came back for 9s: stalled")
	}
	c.waitFrom.Store(now.Add(-time.Second).UnixNano())
	if c.stalled(3 * time.Second) {
		t.Fatal("a keepalive sent a second ago after a quiet spell has not waited long yet")
	}
	nc := &nodeConn{}
	nc.lastWrite.Store(now.Add(-20 * time.Second).UnixNano())
	nc.lastRead.Store(now.Add(-8 * time.Second).UnixNano())
	if nc.awaitingAnswer() {
		t.Fatal("the last bytes came from the far end: nothing is pending")
	}
	nc.lastWrite.Store(now.Add(-6 * time.Second).UnixNano())
	if !nc.awaitingAnswer() {
		t.Fatal("the client wrote after the last answer: it is waiting")
	}
}

// probeSeq feeds scheduled probe results the way probe() records them.
func probeSeq(g *smartGroup, tag string, times ...uint16) {
	for _, ms := range times {
		g.health.recordProbe(tag, true, g.health.isStall(tag, testLink, ms))
		alive(g, tag, ms)
	}
}

func repeat(ms uint16, n int) []uint16 {
	out := make([]uint16, n)
	for i := range out {
		out[i] = ms
	}
	return out
}

func TestStallingNodeLosesToSteadyOne(t *testing.T) {
	g, _ := newTestGroup(t, []string{"stally", "steady"}, smartOptions{tolerance: 150})
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return map[string]uint16{"stally": 400, "steady": 450}[out.Tag()], 204, nil
	}
	probeSeq(g, "stally", repeat(400, 5)...)
	probeSeq(g, "steady", repeat(450, 5)...)
	g.reselect()
	if g.Now() != "stally" {
		t.Fatalf("setup: the faster median leads, got %s", g.Now())
	}
	// FI#3 on 2026-09-24: fine most of the time, 1.5-3s now and then
	seq := repeat(400, 11)
	seq[3], seq[7], seq[10] = 2500, 2800, 1900
	probeSeq(g, "stally", seq...)
	probeSeq(g, "steady", repeat(450, 11)...)
	g.reselect()
	if g.Now() != "steady" {
		t.Fatalf("three stalls in sixteen probes must cost more than 50 ms of median, got %s", g.Now())
	}
}

func TestOneStallIsNoReasonToMove(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 150})
	probeSeq(g, "a", repeat(400, 15)...)
	probeSeq(g, "b", repeat(450, 16)...)
	g.reselect()
	probeSeq(g, "a", 2600)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("a single stall must not move the group, got %s", g.Now())
	}
}

func TestLostRacesDemoteThePrimary(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 150})
	g.measure = func(out adapter.Outbound) (uint16, int, error) {
		return 200, 204, nil
	}
	probeSeq(g, "a", repeat(200, 10)...)
	probeSeq(g, "b", repeat(200, 10)...)
	g.reselect()
	quiet, _ := g.fitAs(manager(g, "b"), false)
	real := &g.health.stats("a").real
	for i := 0; i < 40; i++ {
		real.primaryTries.add(1, realHalfLife)
		if i%4 == 0 {
			// a backup started later answered first on the same site
			real.primaryLost.add(1, realHalfLife)
		}
	}
	lossy, _ := g.fitAs(manager(g, "a"), false)
	if lossy < quiet+150 {
		t.Fatalf("losing a quarter of the races must cost more than the tolerance: %v vs %v", lossy, quiet)
	}
	if quiet != 200 {
		t.Fatalf("a node that carried no traffic is average, not penalised: %v", quiet)
	}
}

func manager(g *smartGroup, tag string) adapter.Outbound {
	out, _ := g.manager.Outbound(tag)
	return out
}

func TestOneEarlyFailureIsNoReasonToMove(t *testing.T) {
	g, _ := newTestGroup(t, []string{"head", "other"}, smartOptions{tolerance: 150})
	probeSeq(g, "head", repeat(150, 4)...)
	g.health.recordProbe("head", false, false)
	probeSeq(g, "head", repeat(150, 3)...)
	probeSeq(g, "other", repeat(140, 8)...)
	g.reselect()
	if g.Now() != "head" {
		t.Fatalf("one failed probe among the first eight must not cost more than the tolerance, got %s", g.Now())
	}
	score, _ := g.fitAs(manager(g, "head"), false)
	if score > 150+150 {
		t.Fatalf("one early failure costs too much: %v", score)
	}
}

func TestStallMidAnswerIsRecorded(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a"}, smartOptions{})
	stalls := &g.health.stats("a").stalls
	now := time.Now()
	// a conn 200 KB into an answer whose last data came at prev; the client acknowledged it
	// at once (an HTTP/2 window update) and wrote nothing more
	streaming := func(prev time.Time) *nodeConn {
		c := &nodeConn{health: g.health, tag: "a", stats: g.health.stats("a"), site: "video.example", group: "g"}
		c.streak = 200 << 10
		c.lastWrite.Store(prev.Add(50 * time.Millisecond).UnixNano())
		return c
	}
	data := func(c *nodeConn, at time.Time, waited time.Duration, prev time.Time, n int) {
		c.noteGap(at.UnixNano(), waited, prev.UnixNano(), n)
	}

	c := streaming(now.Add(-3 * time.Second))
	data(c, now, 3*time.Second, now.Add(-3*time.Second), 1000)
	if stalls.Load() != 0 {
		t.Fatal("not before the answer carries on")
	}
	data(c, now.Add(10*time.Millisecond), 0, now, 20000)
	if stalls.Load() != 1 {
		t.Fatal("3s of silence in the middle of a 200 KB answer, then more of it, is a stall")
	}

	// the server closing the idle connection after its keep-alive timeout: one small last
	// frame and nothing more (Instagram's CDN, 65s after each segment)
	c = streaming(now.Add(-65 * time.Second))
	data(c, now, 65*time.Second, now.Add(-65*time.Second), 40)
	if stalls.Load() != 1 {
		t.Fatal("a goodbye after a long silence is no stall")
	}
	// the next request's answer on the same conn is not the old answer carrying on
	c.lastWrite.Store(now.Add(time.Second).UnixNano())
	data(c, now.Add(1100*time.Millisecond), 100*time.Millisecond, now, 30000)
	if stalls.Load() != 1 {
		t.Fatal("data after a new request answers that request")
	}

	// the pause follows a new request: the player asking for the next segment
	c = streaming(now.Add(-4 * time.Second))
	c.lastWrite.Store(now.Add(-time.Second).UnixNano())
	data(c, now, 4*time.Second, now.Add(-4*time.Second), 20000)
	if stalls.Load() != 1 {
		t.Fatal("a pause before a new request is the player pacing itself, not a stall")
	}

	// a small answer followed by silence is a finished response
	c = streaming(now.Add(-4 * time.Second))
	c.streak = 4 << 10
	data(c, now, 4*time.Second, now.Add(-4*time.Second), 20000)
	if stalls.Load() != 1 {
		t.Fatal("silence after a small answer is not a stall")
	}

	// a minute since the last data, but the read returned at once: the player stopped
	// reading with its buffer full and the node was held back
	c = streaming(now.Add(-time.Minute))
	data(c, now, time.Millisecond, now.Add(-time.Minute), 20000)
	if stalls.Load() != 1 {
		t.Fatal("a client that stopped reading is no stall of the node")
	}
	events := 0
	for _, e := range g.health.events.list() {
		if e.Kind == "stall" && e.Group == "g" {
			events++
		}
	}
	if events != 1 {
		t.Fatalf("the stall is logged with its group, got %d events", events)
	}
}

func TestUDPStallNeedsAnAnsweredFlowAndASender(t *testing.T) {
	g, manager := newTestGroup(t, []string{"a"}, smartOptions{})
	node := manager.byTag["a"]
	c := &migratingPacketConn{g: g, destination: M.ParseSocksaddr("162.254.192.1:27015")}
	now := time.Now()
	waiting := func() {
		c.lastRx.Store(now.Add(-5 * time.Second).UnixNano())
		c.waitFrom.Store(now.Add(-4800 * time.Millisecond).UnixNano())
		c.lastTx.Store(now.Add(-100 * time.Millisecond).UnixNano())
	}
	waiting()
	c.received(node)
	if g.health.stats("a").udpStalls.Load() != 0 {
		t.Fatal("the first answer of a flow ends no stall")
	}
	waiting()
	c.received(node)
	if g.health.stats("a").udpStalls.Load() != 1 {
		t.Fatal("5s without answers while the game kept sending is a stall")
	}
	// the flow was moved to b during the next silence: the stall is a's
	other := &fakeOutbound{outbound.NewAdapter("vless", "b", []string{N.NetworkUDP}, nil)}
	waiting()
	c.received(other)
	if g.health.stats("a").udpStalls.Load() != 2 || g.health.stats("b").udpStalls.Load() != 0 {
		t.Fatal("a stall belongs to the node that went silent, not the one the flow moved to")
	}
}

func TestStreamStallsCostInTheScore(t *testing.T) {
	g, _ := newTestGroup(t, []string{"stalling", "smooth"}, smartOptions{})
	probeSeq(g, "stalling", repeat(100, 8)...)
	probeSeq(g, "smooth", repeat(100, 8)...)
	now := time.Now()
	for i := 0; i < 20; i++ {
		c := &nodeConn{health: g.health, tag: "stalling", stats: g.health.stats("stalling"), group: "g"}
		c.noteGap(now.UnixNano(), 0, now.UnixNano(), 100<<10)
		if i%2 == 0 {
			// frozen for 3s in the middle of the answer, then more of it
			c.noteGap(now.UnixNano(), 3*time.Second, now.UnixNano(), 1000)
			c.noteGap(now.UnixNano(), 0, now.UnixNano(), 20000)
		}
	}
	if got := g.health.stats("stalling").real.streams.get(realHalfLife); got < 19 {
		t.Fatalf("every stream that got going counts, got %.1f", got)
	}
	stalling, _ := g.fitAs(manager(g, "stalling"), false)
	smooth, _ := g.fitAs(manager(g, "smooth"), false)
	if stalling < smooth+200 {
		t.Fatalf("half the streams freezing mid-answer must cost: %v vs %v", stalling, smooth)
	}
}

// Apple's QUIC flows through direct logged 30s stalls every half minute in prod: the
// client acknowledges an answer at once, stays quiet, and its next keepalive is answered
// at once.
func TestQUICKeepaliveIsNoUDPStall(t *testing.T) {
	g, manager := newTestGroup(t, []string{"a"}, smartOptions{})
	node := manager.byTag["a"]
	c := &migratingPacketConn{g: g, destination: M.ParseSocksaddr("17.250.85.152:443")}
	c.received(node)
	c.sent()
	if c.waitFrom.Load() != 0 {
		t.Fatal("an acknowledgement right after an answer is not waiting for one")
	}
	c.lastRx.Store(time.Now().Add(-30 * time.Second).UnixNano())
	c.sent()
	c.received(node)
	if g.health.stats("a").udpStalls.Load() != 0 {
		t.Fatal("a keepalive answered at once after a quiet half minute is no stall")
	}
	if c.waitFrom.Load() != 0 {
		t.Fatal("the answer ends the wait")
	}
}

// A paused direct video: the player stopped reading, the copier sits in the write to it and
// the node's last bytes are seconds old. That is not the site going silent mid-record.
func TestPausedReaderIsNotACut(t *testing.T) {
	c := &nodeConn{tls: newTLSRecords()}
	c.tls.feed([]byte{23, 3, 3, 0x40, 0x00, 1, 2, 3})
	c.lastRead.Store(time.Now().Add(-10 * time.Second).UnixNano())
	if c.cutStalled(cutIdle) {
		t.Fatal("no read is pending: the far end is held back, not silent")
	}
	c.reading.Store(time.Now().Add(-time.Second).UnixNano())
	if c.cutStalled(cutIdle) {
		t.Fatal("the read has waited only a second")
	}
	c.reading.Store(time.Now().Add(-5 * time.Second).UnixNano())
	if !c.cutStalled(cutIdle) {
		t.Fatal("5s waiting on the far end inside a record is a cut")
	}
}

func TestSuddenSilenceTriggersACheck(t *testing.T) {
	s := &nodeStats{}
	for _, second := range []struct {
		rate int64
		open bool
		want bool
	}{
		{500 << 10, true, false}, {800 << 10, true, false}, {0, true, false}, {0, true, true}, {0, true, false},
	} {
		if got := silenceStep(s, second.rate, second.open); got != second.want {
			t.Fatalf("rate %d: got %v", second.rate, got)
		}
	}
	quiet := &nodeStats{}
	for i := 0; i < 5; i++ {
		if silenceStep(quiet, 0, true) {
			t.Fatal("a node that was never busy is idle, not failing")
		}
	}
	trickle := &nodeStats{}
	for _, rate := range []int64{500 << 10, 500 << 10, 1000, 0, 0} {
		if silenceStep(trickle, rate, true) {
			t.Fatal("data still trickling in between resets the watch")
		}
	}
}

func TestBareAddressesAreNotJudgedForSilence(t *testing.T) {
	if !ipSite("178.154.239.15") || !ipSite("2a02:6b8::2:242") || ipSite("api.ozon.ru") {
		t.Fatal("ipSite must tell addresses from names")
	}
}

func TestPenaltiesFade(t *testing.T) {
	var d decayed
	d.add(2, flapHalfLife)
	d.at = d.at.Add(-flapHalfLife)
	if v := d.get(flapHalfLife); v < 0.99 || v > 1.01 {
		t.Fatalf("after one half-life two deaths weigh one, got %v", v)
	}
}

func TestLossyNodeLosesPrimaryWithoutDying(t *testing.T) {
	g, _ := newTestGroup(t, []string{"lossy", "good"}, smartOptions{tolerance: 100})
	alive(g, "lossy", 30)
	alive(g, "good", 90)
	g.reselect()
	if g.Now() != "lossy" {
		t.Fatalf("setup: expected lossy primary, got %s", g.Now())
	}
	// a third of its probes fail, never three in a row: it stays alive throughout
	for i := 0; i < 8; i++ {
		ok := i%3 != 0
		g.health.recordProbe("lossy", ok, false)
		if ok {
			alive(g, "lossy", 30)
		} else {
			g.health.Failure("lossy", deadAfterFailures)
		}
	}
	if g.health.State("lossy") == stateDead {
		t.Fatal("setup: the lossy node must not be dead")
	}
	g.reselect()
	if g.Now() != "good" {
		t.Fatalf("a node failing a third of its probes must not stay primary, got %s", g.Now())
	}
	for i := 0; i < 8; i++ {
		g.health.recordProbe("lossy", true, false)
	}
	if g.health.degraded("lossy") {
		t.Fatal("clean probes must clear the degraded mark")
	}
}
