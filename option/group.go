package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string `json:"outbounds" reference:"outbound"`
	Default                   string   `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool     `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds" reference:"outbound"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`

	// fork: all optional, defaults give the fast-failover behavior
	FastInterval   badoption.Duration `json:"fast_interval,omitempty"`
	ExpectedStatus []int              `json:"expected_status,omitempty"`
	DisableRace    bool               `json:"disable_race,omitempty"`
	ExcludeEgress  []string           `json:"exclude_egress,omitempty"`
	EgressURL      string             `json:"egress_url,omitempty"`
	MinSpeed       int                `json:"min_speed,omitempty"`
	SpeedURL       string             `json:"speed_url,omitempty"`
	Mode           string             `json:"mode,omitempty"`
	SiteMemory     badoption.Duration `json:"site_memory,omitempty"`
	HedgeDelay     badoption.Duration `json:"hedge_delay,omitempty"`
	LastResort     []string           `json:"last_resort,omitempty"`
}
