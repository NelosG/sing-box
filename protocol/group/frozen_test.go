package group

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// streamOn opens a tracked stream through tag whose far end sent the start of a TLS record
// and nothing since: a read has been waiting on it for silent. atBoundary leaves it between
// records instead, as after a finished answer.
func streamOn(t *testing.T, g *smartGroup, tag string, silent time.Duration, atBoundary bool) *nodeConn {
	t.Helper()
	relaySide, site := net.Pipe()
	t.Cleanup(func() { site.Close() })
	c := newNodeConn(g.health, tag, relaySide)
	out, _ := g.manager.Outbound(tag)
	c.site, c.group, c.watchStalls = "video.example", g.tag, true
	c.proxy, c.socks = proxyNode(out), out.Type() == C.TypeSOCKS
	c.tls = newTLSRecords()
	record := []byte{23, 3, 3, 0x40, 0x00}
	if atBoundary {
		c.tls.feed(append(record, make([]byte, 0x4000)...))
	} else {
		c.tls.feed(append(record, 1, 2, 3))
	}
	since := time.Now().Add(-silent).UnixNano()
	c.lastRead.Store(since)
	c.reading.Store(since)
	return c
}

func closedBy(c *nodeConn) string {
	if !c.closing.Load() {
		return ""
	}
	if why := c.why.Load(); why != nil {
		return *why
	}
	return "client"
}

func TestFrozenStreamsCloseAndPutTheNodeOut(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 80)
	g.reselect()
	if g.Now() != "a" {
		t.Fatalf("setup: a is the faster node, got %s", g.Now())
	}
	first := streamOn(t, g, "a", 3500*time.Millisecond, false)
	second := streamOn(t, g, "a", 3500*time.Millisecond, false)
	g.health.tick()
	for _, c := range []*nodeConn{first, second} {
		if why := closedBy(c); !strings.HasPrefix(why, "engine: frozen, mid-answer") {
			t.Fatalf("two streams of one node frozen mid-record for 3.5s are closed, got %q", why)
		}
	}
	if !g.health.stuck("a") {
		t.Fatal("two frozen streams at once put the node out as a primary")
	}
	if g.Now() != "b" {
		t.Fatalf("the group leaves a node whose traffic froze although its probes pass, got %s", g.Now())
	}
	records := g.health.flows.since(0)
	if len(records) != 2 || !records[0].EndedMid || records[0].MidWaitMs < 3000 {
		t.Fatalf("each frozen stream leaves a flow record with its wait, got %+v", records)
	}
	stuckEvents := 0
	for _, e := range g.health.events.list() {
		if e.Kind == "stuck" && e.Node == "a" {
			stuckEvents++
		}
	}
	if stuckEvents != 1 {
		t.Fatalf("the verdict is logged once, got %d", stuckEvents)
	}
}

func TestOneFrozenStreamGetsLonger(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 80)
	g.reselect()
	lone := streamOn(t, g, "a", 4*time.Second, false)
	g.health.tick()
	if closedBy(lone) != "" {
		t.Fatal("one stream frozen for 4s alone is given until frozenAlone")
	}
	longer := time.Now().Add(-frozenAlone - time.Second).UnixNano()
	lone.lastRead.Store(longer)
	lone.reading.Store(longer)
	g.health.tick()
	if closedBy(lone) == "" {
		t.Fatal("a stream frozen past frozenAlone is closed so the app retries")
	}
	if g.health.stuck("a") || g.Now() != "a" {
		t.Fatal("one frozen stream is no verdict on the node")
	}
	again := streamOn(t, g, "a", frozenAlone+time.Second, false)
	g.health.tick()
	if closedBy(again) == "" || !g.health.stuck("a") || g.Now() != "b" {
		t.Fatalf("a second frozen stream within stuckWindow is: stuck %v, primary %s", g.health.stuck("a"), g.Now())
	}
}

func TestPausedOrFinishedStreamsAreNotFrozen(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 80)
	g.reselect()
	// between records: the answer is complete and the connection waits for the next request
	idle := streamOn(t, g, "a", time.Minute, true)
	// inside a record, but no read is pending: the player stopped taking data
	paused := streamOn(t, g, "a", time.Minute, false)
	paused.reading.Store(0)
	// not TLS: an SSH session idle after a screenful of output, nothing tells it from a cut
	ssh := streamOn(t, g, "a", time.Minute, false)
	ssh.tls = newTLSRecords()
	ssh.tls.feed([]byte("SSH-2.0-OpenSSH_9.6\r\n"))
	ssh.streak = 64 << 10
	g.health.tick()
	if closedBy(idle) != "" || closedBy(paused) != "" || closedBy(ssh) != "" {
		t.Fatalf("idle %q, paused %q, ssh %q: none is frozen", closedBy(idle), closedBy(paused), closedBy(ssh))
	}
	if g.health.stuck("a") {
		t.Fatal("no verdict without frozen streams")
	}
}

// A player that seeks asks again on the connection that froze, here on a record boundary:
// the new request gets no answer at all on a stream that was pouring data.
func TestUnansweredRequestOnABusyStreamIsFrozen(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{tolerance: 100})
	alive(g, "a", 50)
	alive(g, "b", 80)
	g.reselect()
	now := time.Now()
	// run: the last run of data and its rate; asked: the client's last write, and how much
	// it sent since the data
	stream := func(run, rate int64, dataAgo, askedAgo time.Duration, sent int64) *nodeConn {
		c := streamOn(t, g, "a", dataAgo, true)
		c.run.Store(run)
		c.runRate.Store(rate)
		c.lastWrite.Store(now.Add(-askedAgo).UnixNano())
		c.upSinceRead.Store(sent)
		return c
	}
	seek := stream(3<<20, 8<<20, 7*time.Second, 6*time.Second, 300)
	// the window update right after the last data, then nothing: a finished answer, idle
	idle := stream(3<<20, 8<<20, 7*time.Second, 7*time.Second-50*time.Millisecond, 13)
	// a websocket or a long poll that never carried much waits for its answer as long as it likes
	poll := stream(40<<10, 8<<20, 7*time.Second, 6*time.Second, 300)
	// an LLM's event stream of two megabytes, then the next request the API thinks about
	api := stream(2<<20, 15<<10, 7*time.Second, 6*time.Second, 4000)
	// a big page, then an API call made 20s later that the server takes long to answer
	later := stream(3<<20, 8<<20, 26*time.Second, 6*time.Second, 900)
	// a file sent after a download, which the server takes its time to process
	upload := stream(3<<20, 8<<20, 7*time.Second, 6*time.Second, 5<<20)
	g.health.tick()
	if why := closedBy(seek); !strings.HasPrefix(why, "engine: frozen, request unanswered") {
		t.Fatalf("a request unanswered for 6s on a stream that was pouring video is a frozen stream, got %q", why)
	}
	for name, c := range map[string]*nodeConn{"idle": idle, "poll": poll, "api": api, "upload": upload, "later": later} {
		if why := closedBy(c); why != "" {
			t.Fatalf("%s is not frozen, got %q", name, why)
		}
	}
}

func TestFrozenDirectStreamsLeaveDirectInService(t *testing.T) {
	g, _ := newTypedTestGroup(t, []string{"direct", "node"}, map[string]string{"direct": C.TypeDirect}, smartOptions{mode: "fallback"})
	alive(g, "direct", 20)
	alive(g, "node", 80)
	g.reselect()
	first := streamOn(t, g, "direct", 4*time.Second, false)
	second := streamOn(t, g, "direct", 4*time.Second, false)
	g.health.tick()
	if closedBy(first) == "" || closedBy(second) == "" {
		t.Fatal("frozen direct streams are closed too, so the apps retry")
	}
	if g.health.stuck("direct") || g.Now() != "direct" {
		t.Fatal("direct carries every site in the country: frozen streams are those sites, no verdict")
	}
}

func TestFrozenZapretIsLeftAndComesBack(t *testing.T) {
	g, _ := newTypedTestGroup(t, []string{"zapret", "node"}, map[string]string{"zapret": C.TypeSOCKS}, smartOptions{mode: "fallback"})
	alive(g, "zapret", 20)
	alive(g, "node", 80)
	g.reselect()
	streamOn(t, g, "zapret", 4*time.Second, false)
	streamOn(t, g, "zapret", 4*time.Second, false)
	g.health.tick()
	if g.Now() != "node" {
		t.Fatalf("videos freezing on zapret move the group off it, got %s", g.Now())
	}
	g.health.access.Lock()
	g.health.nodes["zapret"].stuckUntil = time.Now().Add(-time.Second)
	g.health.access.Unlock()
	g.reselect()
	if g.Now() != "zapret" {
		t.Fatalf("zapret takes its group back once the time out is over, got %s", g.Now())
	}
}

func TestRepeatedVerdictsLastLonger(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a", "b"}, smartOptions{})
	alive(g, "a", 50)
	alive(g, "b", 80)
	var outs []time.Duration
	for i := 0; i < 3; i++ {
		g.health.noteFreeze("a", "x", true, false)
		g.health.noteFreeze("a", "x", true, false)
		g.health.access.Lock()
		n := g.health.nodes["a"]
		outs = append(outs, time.Until(n.stuckUntil).Round(time.Second))
		n.stuckUntil = time.Time{}
		g.health.access.Unlock()
	}
	if outs[0] != stuckBase || outs[1] != 2*stuckBase || outs[2] != 4*stuckBase {
		t.Fatalf("a node that keeps freezing stays out longer each time: %v", outs)
	}
	// zapret's socks is the exit its groups exist for: out for stuckBase every time
	g.health.noteFreeze("zapret", "x", true, true)
	g.health.noteFreeze("zapret", "x", true, true)
	g.health.access.Lock()
	out := time.Until(g.health.nodes["zapret"].stuckUntil).Round(time.Second)
	g.health.access.Unlock()
	if out != stuckBase {
		t.Fatalf("zapret stays out for %v", out)
	}
}

func TestUnansweredRequestsNeedTwoSitesForAVerdict(t *testing.T) {
	g, _ := newTestGroup(t, []string{"a"}, smartOptions{})
	alive(g, "a", 50)
	// two slow answers from one API after big pages: that site
	g.health.noteFreeze("a", "api.example", false, false)
	g.health.noteFreeze("a", "api.example", false, false)
	if g.health.stuck("a") {
		t.Fatal("two unanswered requests to one site are that site")
	}
	g.health.noteFreeze("a", "cdn.example", false, false)
	if !g.health.stuck("a") {
		t.Fatal("unanswered requests on two sites at once are the node")
	}
}

func TestSiblingOfANodeThatDiedIsNoNewPrimary(t *testing.T) {
	g, _ := newTestGroup(t, []string{"FI #3 | VLESS", "FI #3 | HY2", "LV | VLESS"}, smartOptions{tolerance: 50})
	alive(g, "FI #3 | VLESS", 50)
	alive(g, "FI #3 | HY2", 60)
	alive(g, "LV | VLESS", 200)
	g.reselect()
	if g.Now() != "FI #3 | VLESS" {
		t.Fatalf("setup: got %s", g.Now())
	}
	// it dies carrying a stream
	streamOn(t, g, "FI #3 | VLESS", 0, true)
	kill(g, "FI #3 | VLESS")
	if g.Now() != "LV | VLESS" {
		t.Fatalf("the dead node's own server is skipped for a while, got %s", g.Now())
	}
	cands := g.candidates(N.NetworkTCP)
	if cands[0].Tag() != "LV | VLESS" || cands[len(cands)-1].Tag() != "FI #3 | VLESS" {
		t.Fatalf("race order after the death: %v", tags(cands))
	}
	if siblingKey("NL | SS") != "NL" || siblingKey("direct") != "direct" {
		t.Fatal("siblingKey takes the server part of a provider tag")
	}
}

func TestRaceBackupsPreferAnotherServer(t *testing.T) {
	g, _ := newTestGroup(t, []string{"FI | VLESS", "FI | HY2", "LV | VLESS"}, smartOptions{tolerance: 50})
	alive(g, "FI | VLESS", 50)
	alive(g, "FI | HY2", 60)
	alive(g, "LV | VLESS", 90)
	g.reselect()
	cands := g.candidates(N.NetworkTCP)
	if got := tags(cands); got[0] != "FI | VLESS" || got[1] != "LV | VLESS" {
		t.Fatalf("the first backup of a primary is on another server, got %v", got)
	}
}

func tags(outs []adapter.Outbound) []string {
	var result []string
	for _, out := range outs {
		result = append(result, out.Tag())
	}
	return result
}

// udpOutbound hands out working loopback UDP sockets.
type udpOutbound struct {
	outbound.Adapter
}

func (o *udpOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (o *udpOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

func TestStalledFlowOnAHealthyNodeIsRenewedInPlace(t *testing.T) {
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"a": &udpOutbound{outbound.NewAdapter("hysteria2", "a", []string{N.NetworkUDP}, nil)},
		"b": &udpOutbound{outbound.NewAdapter("hysteria2", "b", []string{N.NetworkUDP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, []string{"a", "b"}, smartOptions{link: testLink, tolerance: 50})
	t.Cleanup(func() { g.Close() })
	g.headProbed.Store(true)
	g.settled.Store(true)
	alive(g, "a", 50)
	alive(g, "b", 90)
	g.reselect()
	pc, err := newMigratingPacketConn(context.Background(), g, M.ParseSocksaddr("157.240.1.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	flow := pc.(*migratingPacketConn)
	t.Cleanup(func() { flow.Close() })
	stall := func() {
		now := time.Now()
		flow.answered.Store(true)
		flow.lastRx.Store(now.Add(-5 * time.Second).UnixNano())
		flow.waitFrom.Store(now.Add(-4 * time.Second).UnixNano())
		flow.lastTx.Store(now.Add(-200 * time.Millisecond).UnixNano())
		g.health.tick()
	}
	waitMoves := func(want int) string {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			flow.access.RLock()
			moves, node := flow.moves, flow.node.Tag()
			flow.access.RUnlock()
			if moves == want {
				return node
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("expected move %d of the stalled flow", want)
		return ""
	}
	stall()
	if node := waitMoves(1); node != "a" {
		t.Fatalf("a flow stalled on a node that looks fine is renewed there, it went to %s", node)
	}
	if flow.waitFrom.Load() != 0 {
		t.Fatal("the renewal ends the wait it was made for")
	}
	// still no answer on the fresh session (a VLESS node hands XUDP the old socket): move on
	stall()
	if node := waitMoves(2); node != "b" {
		t.Fatalf("a flow still stalled after its renewal leaves the node, got %s", node)
	}
}

func TestSiteFailureBehindAWorkingNodeIsNotTheNode(t *testing.T) {
	refused := remoteErr("remote error: dial tcp4 194.221.250.50:80: connect: connection refused")
	for _, c := range []struct {
		r    attemptResult
		want bool
	}{
		{attemptResult{err: refused}, false},
		{attemptResult{err: net.ErrClosed, connected: true}, false},
		{attemptResult{err: context.DeadlineExceeded}, true},
		{attemptResult{err: net.UnknownNetworkError("handshake")}, true},
		{attemptResult{err: remoteErr("remote error: tls: bad certificate")}, true},
	} {
		if got := nodeFault(c.r); got != c.want {
			t.Fatalf("%v connected=%v: nodeFault %v", c.r.err, c.r.connected, got)
		}
	}
}

type remoteErr string

func (e remoteErr) Error() string { return string(e) }
