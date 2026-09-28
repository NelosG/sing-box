package option

import "github.com/sagernet/sing/common/json/badoption"

// FallbackDNSServerOptions configures the fork's "fallback" DNS server: an ordered list of
// other servers, the next one asked when the current one has not answered within Delay.
type FallbackDNSServerOptions struct {
	Servers badoption.Listable[string] `json:"servers"`
	Delay   badoption.Duration         `json:"delay,omitempty"`
}
