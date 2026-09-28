package group

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
)

const defaultEgressURL = "https://www.cloudflare.com/cdn-cgi/trace"

// probeEgress asks a geo service which country the node exits in. It understands the
// Cloudflare trace format (a "loc=XX" line) and plain bodies that are just the code.
func probeEgress(ctx context.Context, link string, detour N.Dialer, timeout time.Duration) (string, error) {
	if link == "" {
		link = defaultEgressURL
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return "", err
	}
	port := linkURL.Port()
	if port == "" {
		port = "443"
		if linkURL.Scheme == "http" {
			port = "80"
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	instance, err := detour.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPortStr(linkURL.Hostname(), port))
	if err != nil {
		return "", err
	}
	defer instance.Close()
	stop := context.AfterFunc(ctx, func() { instance.Close() })
	defer stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return "", err
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if code, found := strings.CutPrefix(line, "loc="); found {
			line = code
		}
		if len(line) == 2 && strings.ToUpper(line) == line {
			return line, nil
		}
	}
	return "", E.New("no country in egress answer from ", link)
}

const defaultSpeedURL = "https://speed.cloudflare.com/__down?bytes=5000000"

// The rate is taken once TCP's slow start is behind: from speedSkip after the first byte to
// the end of the transfer, which stops after speedSpan. Timed from the first byte, 2 MB was
// mostly slow start, and a 300 Mbit node measured like a 50 Mbit one.
const (
	speedSkip = 400 * time.Millisecond
	speedSpan = 2500 * time.Millisecond
)

// probeSpeed downloads link through detour and returns bytes per second.
func probeSpeed(ctx context.Context, link string, detour N.Dialer, timeout time.Duration) (int64, error) {
	if link == "" {
		link = defaultSpeedURL
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return detour.DialContext(ctx, network, M.ParseSocksaddr(addr))
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
	}
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	buffer := make([]byte, 32*1024)
	var total, markBytes int64
	var first, mark time.Time
	for {
		n, readErr := resp.Body.Read(buffer)
		now := time.Now()
		if n > 0 && first.IsZero() {
			first = now
		}
		total += int64(n)
		if !first.IsZero() {
			if mark.IsZero() && now.Sub(first) >= speedSkip {
				mark, markBytes = now, total
			}
			if now.Sub(first) >= speedSpan {
				break
			}
		}
		if readErr != nil {
			if readErr != io.EOF && total == 0 {
				return 0, readErr
			}
			break
		}
	}
	if first.IsZero() || total < 64*1024 {
		return 0, E.New("speed test through ", link, " returned too little data")
	}
	// a node that delivered everything before speedSkip is timed over the whole transfer
	from, bytes := first, total
	if !mark.IsZero() && total-markBytes >= 64*1024 {
		from, bytes = mark, total-markBytes
	}
	elapsed := time.Since(from)
	if elapsed <= 0 {
		return 0, E.New("speed test through ", link, " took no time")
	}
	return int64(float64(bytes) / elapsed.Seconds()), nil
}

// probeLink runs one HEAD request to link through detour and returns the elapsed time and
// the HTTP status. Unlike urltest.URLTest it reports the status, so a geo-block answer
// (a perfectly fast 403) can be told apart from a healthy node.
func probeLink(ctx context.Context, link string, detour N.Dialer, timeout time.Duration) (uint16, int, error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return 0, 0, err
	}
	port := linkURL.Port()
	if port == "" {
		port = "443"
		if linkURL.Scheme == "http" {
			port = "80"
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	instance, err := detour.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPortStr(linkURL.Hostname(), port))
	if err != nil {
		return 0, 0, err
	}
	defer instance.Close()
	// Closing on cancel unblocks reads on conns that ignore the context.
	stop := context.AfterFunc(ctx, func() { instance.Close() })
	defer stop()
	// The dial counts: it is what every new connection pays. Upstream restarts the clock after
	// it for conns that send their header on the first write, which hid the TCP and TLS
	// handshakes VLESS and Trojan make with the node for every connection (two round trips)
	// while an HY2 stream on a live QUIC connection costs none, so the score could not see
	// the difference a browser opening a page feels.
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, nil)
	if err != nil {
		return 0, 0, err
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, 0, E.Cause(ctx.Err(), "probe ", link)
		}
		return 0, 0, err
	}
	resp.Body.Close()
	elapsed := time.Since(start) / time.Millisecond
	if elapsed > 65535 {
		elapsed = 65535
	}
	if elapsed == 0 {
		elapsed = 1
	}
	return uint16(elapsed), resp.StatusCode, nil
}
