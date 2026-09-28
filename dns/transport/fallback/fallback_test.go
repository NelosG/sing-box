package fallback

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"

	mDNS "github.com/miekg/dns"
)

type fakeServer struct {
	dns.TransportAdapter
	delay time.Duration
	rcode int
	fail  bool
	calls atomic.Int32
}

func newFake(tag string, delay time.Duration, rcode int, fail bool) *fakeServer {
	return &fakeServer{TransportAdapter: dns.NewTransportAdapter("fake", tag, nil), delay: delay, rcode: rcode, fail: fail}
}

func (f *fakeServer) Start(adapter.StartStage) error { return nil }
func (f *fakeServer) Close() error                   { return nil }
func (f *fakeServer) Reset()                         {}

func (f *fakeServer) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	f.calls.Add(1)
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.fail {
		return nil, context.DeadlineExceeded
	}
	response := new(mDNS.Msg)
	response.SetRcode(message, f.rcode)
	return response, nil
}

func (f *fakeServer) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(*mDNS.Msg, error)) {
	go func() { callback(f.Exchange(ctx, message)) }()
}

func newTransport(members ...*fakeServer) *Transport {
	t := &Transport{
		TransportAdapter: dns.NewTransportAdapter(Type, "fb", nil),
		logger:           log.NewNOPFactory().Logger(),
		hedge:            50 * time.Millisecond,
		penalty:          map[string]time.Time{},
	}
	for _, m := range members {
		t.members = append(t.members, m)
		t.tags = append(t.tags, m.Tag())
	}
	return t
}

func query() *mDNS.Msg {
	m := new(mDNS.Msg)
	m.SetQuestion("ya.ru.", mDNS.TypeA)
	return m
}

func TestFirstServerAnswersAlone(t *testing.T) {
	a, b := newFake("a", 0, mDNS.RcodeSuccess, false), newFake("b", 0, mDNS.RcodeSuccess, false)
	response, err := newTransport(a, b).Exchange(context.Background(), query())
	if err != nil || response.Rcode != mDNS.RcodeSuccess {
		t.Fatalf("unexpected %v %v", response, err)
	}
	if b.calls.Load() != 0 {
		t.Fatal("a fast healthy first server must not wake the second")
	}
}

func TestRefusedGoesToNextAndIsPenalised(t *testing.T) {
	router, doh := newFake("router", 0, mDNS.RcodeRefused, false), newFake("doh", 0, mDNS.RcodeSuccess, false)
	tr := newTransport(router, doh)
	response, err := tr.Exchange(context.Background(), query())
	if err != nil || response.Rcode != mDNS.RcodeSuccess {
		t.Fatalf("REFUSED must fall through to the next server, got %v %v", response, err)
	}
	if order := tr.order(); order[0].Tag() != "doh" {
		t.Fatal("a refusing server must go to the back of the queue")
	}
}

func TestSlowServerIsHedged(t *testing.T) {
	slow, fast := newFake("slow", 2*time.Second, mDNS.RcodeSuccess, false), newFake("fast", 0, mDNS.RcodeSuccess, false)
	start := time.Now()
	_, err := newTransport(slow, fast).Exchange(context.Background(), query())
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("hedge must not wait for the slow server, took %v", elapsed)
	}
}

func TestNXDomainIsAnAnswer(t *testing.T) {
	a, b := newFake("a", 0, mDNS.RcodeNameError, false), newFake("b", 0, mDNS.RcodeSuccess, false)
	response, _ := newTransport(a, b).Exchange(context.Background(), query())
	if response.Rcode != mDNS.RcodeNameError || b.calls.Load() != 0 {
		t.Fatal("NXDOMAIN is a verdict about the name, not a server failure")
	}
}

func TestAllFailingReturnsLastRealResponse(t *testing.T) {
	a, b := newFake("a", 0, mDNS.RcodeServerFailure, false), newFake("b", 0, 0, true)
	response, err := newTransport(a, b).Exchange(context.Background(), query())
	if err != nil || response == nil || response.Rcode != mDNS.RcodeServerFailure {
		t.Fatalf("expected the SERVFAIL response to be passed on, got %v %v", response, err)
	}
}
