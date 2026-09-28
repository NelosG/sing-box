package group

import (
	"context"
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

// resortGroup is a group of two proxy nodes that cannot reach anything, with direct (which
// answers) as its last resort.
func resortGroup(t *testing.T) *smartGroup {
	t.Helper()
	manager := &fakeManager{byTag: map[string]adapter.Outbound{
		"a":      &fakeOutbound{outbound.NewAdapter("vless", "a", []string{N.NetworkTCP}, nil)},
		"b":      &fakeOutbound{outbound.NewAdapter("vless", "b", []string{N.NetworkTCP}, nil)},
		"direct": &pipeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP, N.NetworkUDP}, nil)},
	}}
	g := newSmartGroup(context.Background(), log.NewNOPFactory().Logger(), "g", urltest.NewHistoryStorage(), manager, []string{"a", "b"},
		smartOptions{link: testLink, tolerance: 50, lastResort: []string{"direct"}})
	t.Cleanup(func() { g.Close() })
	g.headProbed.Store(true)
	g.settled.Store(true)
	alive(g, "a", 50)
	alive(g, "b", 80)
	alive(g, "direct", 10)
	g.reselect()
	return g
}

func dialSite(t *testing.T, g *smartGroup, site string, resortOK bool) (adapter.Outbound, error) {
	t.Helper()
	conn, err := g.dialRace(context.Background(), adapter.InboundContext{Destination: M.ParseSocksaddr(site + ":443")}, nil, true, resortOK)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out, _ := g.manager.Outbound(conn.(*nodeConn).tag)
	return out, nil
}

func TestLastResortAnswersWhenNoMemberDoes(t *testing.T) {
	g := resortGroup(t)
	out, err := dialSite(t, g, "animego.example", true)
	if err != nil || out.Tag() != "direct" {
		t.Fatalf("with every member failing the last resort carries the site, got %v %v", out, err)
	}
	if _, found := g.resortSites.Load("animego.example"); !found {
		t.Fatal("a site that answered only through the last resort goes there first for a while")
	}
	logged := 0
	for _, e := range g.health.events.list() {
		if e.Kind == "site_fallback" {
			logged++
		}
	}
	if logged != 1 {
		t.Fatalf("the fallback is logged once, got %d", logged)
	}
	// the remembered site: direct first, and direct is not checked against the group's link
	g.verifying.Range(func(key, _ any) bool { g.verifying.Delete(key); return true })
	out, err = dialSite(t, g, "animego.example", true)
	if err != nil || out.Tag() != "direct" {
		t.Fatalf("the remembered last resort carries the site again, got %v %v", out, err)
	}
	if _, running := g.verifying.Load("direct"); running {
		t.Fatal("the last resort is no member and must never be probed on the group's link")
	}
	if g.health.State("direct") != stateAlive {
		t.Fatal("direct stays alive")
	}
}

func TestLastResortOnlyForRoutedConnections(t *testing.T) {
	g := resortGroup(t)
	if _, err := dialSite(t, g, "probe.example", false); err == nil {
		t.Fatal("a group dialed by a probe or another group reports its members' failure, it does not fall back")
	}
}

func TestMemberAnsweringAgainEndsTheFallback(t *testing.T) {
	g := resortGroup(t)
	dialSite(t, g, "animego.example", true)
	// b gets through again: it answers the site as soon as it is raced behind direct
	g.manager.(*fakeManager).byTag["b"] = &pipeOutbound{outbound.NewAdapter("vless", "b", []string{N.NetworkTCP}, nil)}
	g.resortSites.Store("animego.example", siteChoice{tag: "direct", until: time.Now().Add(time.Minute)})
	g.manager.(*fakeManager).byTag["direct"] = &fakeOutbound{outbound.NewAdapter(C.TypeDirect, "direct", []string{N.NetworkTCP}, nil)}
	out, err := dialSite(t, g, "animego.example", true)
	if err != nil || out.Tag() != "b" {
		t.Fatalf("a member takes the site back, got %v %v", out, err)
	}
	if _, found := g.resortSites.Load("animego.example"); found {
		t.Fatal("the memory of the fallback ends once a member answers")
	}
}

// auto-direct as direct, zapret, the pool: a site silent on zapret tries the pool before it is
// pinned back to direct as a site that simply answers like that.
func TestSiteWalksTheWholeChainBeforeItIsPinnedBack(t *testing.T) {
	g, manager := newTypedTestGroup(t, []string{"direct", "zapret", "pool"},
		map[string]string{"direct": C.TypeDirect, "zapret": C.TypeSOCKS, "pool": C.TypeURLTest}, smartOptions{mode: "fallback"})
	for _, tag := range []string{"direct", "zapret", "pool"} {
		alive(g, tag, 20)
	}
	g.reselect()
	direct, zapret, pool := manager.byTag["direct"], manager.byTag["zapret"], manager.byTag["pool"]
	choice := func() siteChoice {
		value, _ := g.sites.Load("site.example")
		return value.(siteChoice)
	}
	for i := 0; i < siteStrikeLimit; i++ {
		g.siteMoveFailed("site.example", direct, zapret)
	}
	if c := choice(); c.tag != "pool" || !c.strict {
		t.Fatalf("the same silence on zapret moves the site on to the pool, got %+v", c)
	}
	for i := 0; i < siteStrikeLimit; i++ {
		g.siteMoveFailed("site.example", direct, pool)
	}
	if c := choice(); c.tag != "direct" || !c.pinned {
		t.Fatalf("the same silence everywhere pins the site back to direct, got %+v", c)
	}
	// a stream frozen on zapret moves the site on at once
	g.sites.Delete("other.example")
	g.siteFrozen("other.example", direct, zapret)
	if value, _ := g.sites.Load("other.example"); value.(siteChoice).tag != "pool" {
		t.Fatalf("frozen on zapret moves on, got %+v", value)
	}
	// and the pool winning a later race keeps the move strict
	g.remember("other.example", zapret, pool)
	g.sites.Store("third.example", siteChoice{tag: "zapret", until: time.Now().Add(time.Hour), strict: true})
	g.remember("third.example", zapret, pool)
	if value, _ := g.sites.Load("third.example"); !value.(siteChoice).strict || value.(siteChoice).tag != "pool" {
		t.Fatalf("a strict move stays strict when the next exit wins the race, got %+v", value)
	}
}
