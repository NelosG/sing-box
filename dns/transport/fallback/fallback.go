// Package fallback is a DNS server made of other DNS servers. sing-box can only switch
// servers through rules, so a default server that goes down (or starts answering REFUSED,
// as a home router does now and then) takes every lookup with it. This one asks its first
// healthy member, brings in the next one if no good answer arrives within a short delay,
// and keeps a failing member at the back of the queue for a while.
package fallback

import (
	"context"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

const Type = "fallback"

const (
	defaultHedge = 400 * time.Millisecond
	penaltyTime  = 30 * time.Second
)

func RegisterTransport(registry *dns.TransportRegistry) {
	dns.RegisterTransport[option.FallbackDNSServerOptions](registry, Type, NewTransport)
}

var _ adapter.DNSTransport = (*Transport)(nil)

type Transport struct {
	dns.TransportAdapter
	ctx    context.Context
	logger log.ContextLogger
	tags   []string
	hedge  time.Duration

	access  sync.Mutex
	members []adapter.DNSTransport
	penalty map[string]time.Time
}

func NewTransport(ctx context.Context, logger log.ContextLogger, tag string, options option.FallbackDNSServerOptions) (adapter.DNSTransport, error) {
	if len(options.Servers) == 0 {
		return nil, E.New("fallback dns server needs at least one server")
	}
	for _, server := range options.Servers {
		if server == tag {
			return nil, E.New("fallback dns server ", tag, " lists itself")
		}
	}
	hedge := time.Duration(options.Delay)
	if hedge <= 0 {
		hedge = defaultHedge
	}
	return &Transport{
		// Members are dependencies, so the manager starts them first.
		TransportAdapter: dns.NewTransportAdapter(Type, tag, options.Servers),
		ctx:              ctx,
		logger:           logger,
		tags:             options.Servers,
		hedge:            hedge,
		penalty:          map[string]time.Time{},
	}, nil
}

func (t *Transport) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	manager := service.FromContext[adapter.DNSTransportManager](t.ctx)
	if manager == nil {
		return E.New("missing dns transport manager")
	}
	for _, tag := range t.tags {
		member, loaded := manager.Transport(tag)
		if !loaded {
			return E.New("fallback dns server ", t.Tag(), ": server not found: ", tag)
		}
		t.members = append(t.members, member)
	}
	return nil
}

func (t *Transport) Close() error {
	return nil
}

func (t *Transport) Reset() {
	for _, member := range t.members {
		member.Reset()
	}
}

// order puts members in configured order, penalised ones last.
func (t *Transport) order() []adapter.DNSTransport {
	t.access.Lock()
	defer t.access.Unlock()
	now := time.Now()
	healthy := make([]adapter.DNSTransport, 0, len(t.members))
	var penalised []adapter.DNSTransport
	for _, member := range t.members {
		if until, found := t.penalty[member.Tag()]; found && now.Before(until) {
			penalised = append(penalised, member)
			continue
		}
		healthy = append(healthy, member)
	}
	return append(healthy, penalised...)
}

func (t *Transport) punish(tag string) {
	t.access.Lock()
	t.penalty[tag] = time.Now().Add(penaltyTime)
	t.access.Unlock()
}

func (t *Transport) forgive(tag string) {
	t.access.Lock()
	delete(t.penalty, tag)
	t.access.Unlock()
}

type answer struct {
	member   adapter.DNSTransport
	response *mDNS.Msg
	err      error
}

// good rejects answers that mean "this server cannot help", as opposed to a real verdict
// about the name such as NXDOMAIN.
func good(a answer) bool {
	if a.err != nil || a.response == nil {
		return false
	}
	switch a.response.Rcode {
	case mDNS.RcodeServerFailure, mDNS.RcodeRefused:
		return false
	}
	return true
}

func (t *Transport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	members := t.order()
	if len(members) == 0 {
		return nil, E.New("fallback dns server ", t.Tag(), " not started")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	answers := make(chan answer, len(members))
	next := 0
	launch := func() {
		member := members[next]
		next++
		go func() {
			response, err := member.Exchange(ctx, message.Copy())
			answers <- answer{member, response, err}
		}()
	}
	launch()
	timer := time.NewTimer(t.hedge)
	defer timer.Stop()
	var last answer
	for received := 0; received < next; {
		select {
		case a := <-answers:
			received++
			if good(a) {
				t.forgive(a.member.Tag())
				return a.response, nil
			}
			if ctx.Err() == nil {
				t.punish(a.member.Tag())
				t.logger.DebugContext(ctx, "server ", a.member.Tag(), " failed, trying the next one")
			}
			if a.response != nil || last.response == nil {
				last = a
			}
			if next < len(members) {
				launch()
				timer.Reset(t.hedge)
			}
		case <-timer.C:
			if next < len(members) {
				launch()
				timer.Reset(t.hedge)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Nobody gave a usable answer: pass on the last real response if any, so the caller
	// sees REFUSED or SERVFAIL rather than a made-up error.
	if last.response != nil {
		return last.response, nil
	}
	return nil, last.err
}

func (t *Transport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(response *mDNS.Msg, err error)) {
	go func() {
		callback(t.Exchange(ctx, message))
	}()
}
