package group

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// migratingPacketConn is a UDP flow that stays on one node while that node lives. When the
// node dies (or the flow breaks under us) the flow is reopened through the next candidate
// and the app keeps sending into the same socket. Games and calls see a new peer address
// and most re-sync on their own; QUIC handles it as a path change. That beats a dropped flow.
type migratingPacketConn struct {
	g           *smartGroup
	ctx         context.Context
	destination M.Socksaddr

	access sync.RWMutex
	inner  net.PacketConn
	// innerN is inner seen through sing's packet interface, which carries the destination
	// as a Socksaddr. Through net.PacketConn alone a domain destination is turned into an
	// empty UDPAddr and XUDP rejects the packet ("unsupported address"): QUIC to a domain
	// died after its first packet. Upstream urltest has the same flaw.
	innerN N.PacketConn
	node   adapter.Outbound
	gen    uint64

	closed atomic.Bool
	lastRx atomic.Int64
	lastTx atomic.Int64
	// the app's first send after the last answer, 0 when it is not waiting. Sends within
	// ackGrace of an answer do not count: QUIC acknowledges the last packet and may then sit
	// quiet until its next keepalive half a minute later, answered at once, and counted from
	// the last answer that looked like a 30s stall.
	waitFrom atomic.Int64
	// node that delivered the last answer; only the reading goroutine touches it
	rxNode adapter.Outbound
	// first write and any answer, for a flow the far end never answers at all
	firstTx    atomic.Int64
	answered   atomic.Bool
	migrations []time.Time
	readDL     time.Time
	writeDL    time.Time

	// for the flow record: when it opened, moves so far (guarded by access) and the longest
	// the app waited for an answer, ns
	opened  time.Time
	moves   int
	maxWait atomic.Int64
	// when the flow was last renewed in place, see renew; a move opening its new session;
	// both guarded by access
	renewedAt time.Time
	moving    bool
}

// A flow that keeps failing wherever it goes is the destination's problem, not ours.
const (
	migrationBurst  = 5
	migrationWindow = time.Minute
)

var _ N.PacketConn = (*migratingPacketConn)(nil)

func newMigratingPacketConn(ctx context.Context, g *smartGroup, destination M.Socksaddr) (net.PacketConn, error) {
	pc, out, err := g.listenSerial(ctx, destination, "")
	if err != nil {
		return nil, err
	}
	c := &migratingPacketConn{g: g, ctx: context.WithoutCancel(ctx), destination: destination, inner: pc, innerN: bufio.NewPacketConn(pc), node: out, opened: time.Now()}
	c.lastRx.Store(time.Now().UnixNano())
	g.health.trackPacket(out.Tag(), c)
	return c, nil
}

func (c *migratingPacketConn) current() (net.PacketConn, adapter.Outbound, uint64) {
	c.access.RLock()
	defer c.access.RUnlock()
	return c.inner, c.node, c.gen
}

// flowFailed handles a read or write error on the flow. The first failure towards a
// destination moves the flow: a node that just went down shows up here before its probes
// do. Flows to that destination failing again within a minute, on whatever node, mean the far
// end or everyone's path to it is the problem, and moving only repeats the failure: a video
// player's peer-to-peer UDP to home connections in Russia failed on every node, each failure
// moved the flow up to five times and the app opened a new flow at once, a stream of new XUDP
// sessions a second on the node carrying the video itself. The node gets a probe either way.
func (c *migratingPacketConn) flowFailed(node adapter.Outbound) bool {
	tag := node.Tag()
	if c.g.health.State(tag) == stateDead || c.g.health.degraded(tag) {
		return c.migrate(tag)
	}
	if c.g.health.probedAgo(tag) >= time.Second {
		c.g.probeAsync(node, false)
	}
	if !c.g.flowWorthMoving(c.destination.String()) {
		return false
	}
	return c.migrate(tag)
}

// migrate moves the flow off the node tagged from; it is a no-op if the flow already left it.
func (c *migratingPacketConn) migrate(from string) bool {
	return c.move(from, from)
}

// renew reopens a stalled flow on a node that looks fine, through whatever the group points
// at now: usually the same node, so the app keeps its address, only on a fresh relay session.
// A flow still stalled after that within a minute moves off the node, if the destination is
// worth a move at all: over VLESS the node knows the flow by its XUDP global ID and hands the
// "new" session its old socket, so renewing there changes nothing.
func (c *migratingPacketConn) renew(from string) bool {
	c.access.RLock()
	renewed := time.Since(c.renewedAt) < migrationWindow
	c.access.RUnlock()
	if !renewed {
		return c.move(from, "")
	}
	if !c.g.flowWorthMoving(c.destination.String()) {
		return false
	}
	return c.move(from, from)
}

// move reopens the flow that is on from through the first candidate other than exclude.
func (c *migratingPacketConn) move(from, exclude string) bool {
	c.access.Lock()
	if c.closed.Load() {
		c.access.Unlock()
		return false
	}
	if c.node.Tag() != from || c.moving {
		c.access.Unlock()
		return true
	}
	now := time.Now()
	recent := c.migrations[:0]
	for _, t := range c.migrations {
		if now.Sub(t) < migrationWindow {
			recent = append(recent, t)
		}
	}
	c.migrations = recent
	if len(c.migrations) >= migrationBurst {
		c.access.Unlock()
		return false
	}
	c.moving = true
	c.access.Unlock()
	// The new session opens while the flow runs on the old one: a node that does not answer
	// holds the open for seconds, and a renewal that tried a dying node first used to hold the
	// app's packets and the dead-node move with them (9s without UDP on the stand).
	pc, out, err := c.g.listenSerial(c.ctx, c.destination, exclude)
	c.access.Lock()
	defer c.access.Unlock()
	c.moving = false
	if err != nil {
		c.g.logger.Debug("udp flow to ", c.destination, " could not leave ", from, ": ", err)
		return false
	}
	if c.closed.Load() || c.node.Tag() != from {
		pc.Close()
		return !c.closed.Load()
	}
	now = time.Now()
	c.migrations = append(c.migrations, now)
	c.moves++
	if exclude == "" {
		c.renewedAt = now
	}
	var waited time.Duration
	if wait := c.waitFrom.Swap(0); wait != 0 {
		// The stall that caused the move ends here as far as judging goes: the app's next send
		// starts the clock again, or the flow would be moved every second. The stall is the
		// silent node's, counted now since the answer will come through another session.
		waited = now.Sub(time.Unix(0, wait))
		storeMax(&c.maxWait, int64(waited))
		if waited >= udpStallMin && c.answered.Load() {
			c.g.health.recordStall(from, c.g.tag, c.destination.String(), waited, true)
		}
	}
	if !c.readDL.IsZero() {
		pc.SetReadDeadline(c.readDL)
	}
	if !c.writeDL.IsZero() {
		pc.SetWriteDeadline(c.writeDL)
	}
	old := c.inner
	c.g.health.untrackPacket(from, c)
	c.inner, c.innerN, c.node = pc, bufio.NewPacketConn(pc), out
	c.gen++
	c.g.health.trackPacket(out.Tag(), c)
	old.Close()
	c.g.logger.Info("udp flow to ", c.destination, " moved ", from, " -> ", out.Tag())
	how := "from " + from
	if exclude == "" {
		how = "renewed, was on " + from
	}
	c.g.health.event("udp_moved", out.Tag(), c.g.tag, fmt.Sprintf("%s to %s (no answer %.1fs)", how, c.destination, waited.Seconds()))
	return true
}

func (c *migratingPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		inner, node, gen := c.current()
		n, addr, err := inner.ReadFrom(p)
		if err == nil {
			c.received(node)
			return n, addr, err
		}
		if c.closed.Load() {
			return n, addr, err
		}
		if E.IsTimeout(err) {
			return n, addr, err
		}
		if _, _, now := c.current(); now != gen {
			continue
		}
		if !c.flowFailed(node) {
			return n, addr, err
		}
	}
}

// LazyHeadroom makes the copier reserve the default header room (1 KB) in every buffer.
// Writers such as XUDP prepend their header in place and panic without room; the room they
// need depends on the node, which can change when the flow moves, so a fixed figure from
// the first node would not do.
func (c *migratingPacketConn) LazyHeadroom() bool {
	return true
}

func (c *migratingPacketConn) currentN() (N.PacketConn, adapter.Outbound, uint64) {
	c.access.RLock()
	defer c.access.RUnlock()
	return c.innerN, c.node, c.gen
}

func (c *migratingPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	for {
		inner, node, gen := c.currentN()
		destination, err := inner.ReadPacket(buffer)
		if err == nil {
			c.received(node)
			return destination, nil
		}
		if c.closed.Load() || E.IsTimeout(err) {
			return destination, err
		}
		if _, _, now := c.currentN(); now != gen {
			continue
		}
		if !c.flowFailed(node) {
			return destination, err
		}
	}
}

// WritePacket hands the buffer to the node; after a failed write the flow moves on and
// that one datagram is lost, as it may be anywhere on the way.
func (c *migratingPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.sent()
	inner, node, _ := c.currentN()
	err := inner.WritePacket(buffer, destination)
	if err == nil || c.closed.Load() || E.IsTimeout(err) {
		return err
	}
	if c.flowFailed(node) {
		return nil
	}
	return err
}

func (c *migratingPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.sent()
	inner, node, _ := c.current()
	n, err := inner.WriteTo(p, addr)
	if err == nil || c.closed.Load() || E.IsTimeout(err) {
		return n, err
	}
	if !c.flowFailed(node) {
		return n, err
	}
	inner, _, _ = c.current()
	return inner.WriteTo(p, addr)
}

// unanswered reports a flow that has sent for at least wait and never heard back.
func (c *migratingPacketConn) unanswered(wait time.Duration) bool {
	first := c.firstTx.Load()
	return first != 0 && !c.answered.Load() && time.Since(time.Unix(0, first)) >= wait
}

// sent notes a datagram from the app.
func (c *migratingPacketConn) sent() {
	now := time.Now().UnixNano()
	c.firstTx.CompareAndSwap(0, now)
	c.lastTx.Store(now)
	if now-c.lastRx.Load() >= int64(ackGrace) {
		c.waitFrom.CompareAndSwap(0, now)
	}
}

// received notes an answer on the flow. An answer the app waited udpStallMin for, on a flow
// that was answered before, ends a stall (a frozen game or call). The stall belongs to the
// node that went silent, which is not the one delivering this answer when the flow was
// moved meanwhile.
func (c *migratingPacketConn) received(node adapter.Outbound) {
	now := time.Now()
	c.lastRx.Store(now.UnixNano())
	wait := c.waitFrom.Swap(0)
	silent := c.rxNode
	if silent == nil {
		silent = node
	}
	c.rxNode = node
	waited := now.Sub(time.Unix(0, wait))
	if wait != 0 {
		storeMax(&c.maxWait, int64(waited))
	}
	if wait != 0 && waited >= udpStallMin && c.answered.Load() &&
		silent.Type() != C.TypeURLTest && silent.Type() != C.TypeSelector {
		c.g.health.recordStall(silent.Tag(), c.g.tag, c.destination.String(), waited, true)
	}
	c.answered.Store(true)
}

// stalled reports a flow the app keeps sending on while nothing has come back for idle: a
// broken path. A flow silent both ways (a paused video, an idle QUIC connection) is not
// stalled; moving those on a suspect node cost Instagram a path change and a 1-2s freeze each.
// Only flows that were answered before count: a peer that never answered (WebRTC checks to
// dead candidates) is not a broken path, and moving it helps nobody.
func (c *migratingPacketConn) stalled(idle time.Duration) bool {
	wait, tx := c.waitFrom.Load(), c.lastTx.Load()
	return c.answered.Load() && wait != 0 && time.Since(time.Unix(0, wait)) >= idle && time.Since(time.Unix(0, tx)) < idle
}

func (c *migratingPacketConn) idleFor() time.Duration {
	return time.Since(time.Unix(0, c.lastRx.Load()))
}

func (c *migratingPacketConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.access.Lock()
	defer c.access.Unlock()
	c.g.health.untrackPacket(c.node.Tag(), c)
	c.record()
	return c.inner.Close()
}

// record files a flow record for a UDP flow that was moved or kept its app waiting; c.access
// is held.
func (c *migratingPacketConn) record() {
	now := time.Now()
	wait := c.maxWait.Load()
	endedWaiting := false
	if from := c.waitFrom.Load(); from != 0 && c.answered.Load() {
		if pending := now.UnixNano() - from; pending >= int64(udpStallMin) {
			endedWaiting = true
			wait = max(wait, pending)
		}
	}
	if c.moves == 0 && wait < int64(udpStallMin) {
		return
	}
	c.g.health.flows.add(FlowRecord{
		Net: "udp", Group: c.g.tag, Site: c.destination.String(), Node: c.node.Tag(), AgeMs: now.Sub(c.opened).Milliseconds(),
		MidWaitMs: wait / int64(time.Millisecond), EndedMid: endedWaiting, Moves: c.moves, Close: "client",
	})
}

func (c *migratingPacketConn) LocalAddr() net.Addr {
	inner, _, _ := c.current()
	return inner.LocalAddr()
}

func (c *migratingPacketConn) SetDeadline(t time.Time) error {
	c.access.Lock()
	c.readDL, c.writeDL = t, t
	inner := c.inner
	c.access.Unlock()
	return inner.SetDeadline(t)
}

func (c *migratingPacketConn) SetReadDeadline(t time.Time) error {
	c.access.Lock()
	c.readDL = t
	inner := c.inner
	c.access.Unlock()
	return inner.SetReadDeadline(t)
}

func (c *migratingPacketConn) SetWriteDeadline(t time.Time) error {
	c.access.Lock()
	c.writeDL = t
	inner := c.inner
	c.access.Unlock()
	return inner.SetWriteDeadline(t)
}
