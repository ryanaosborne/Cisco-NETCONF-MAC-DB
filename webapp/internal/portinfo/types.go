package portinfo

// PortResult is the full interface row joined with VLAN names.
type PortResult struct {
	NodeID          string  `json:"node_id"`
	Name            string  `json:"name"`
	Description     *string `json:"description"`
	Shutdown        bool    `json:"shutdown"`
	IPAddress       *string `json:"ip_address"`
	PrefixLen       *int16  `json:"prefix_len"`
	VRF             *string `json:"vrf"`
	MTU             *int32  `json:"mtu"`
	AccessVlan      int16   `json:"access_vlan"`
	AccessVlanName  *string `json:"access_vlan_name"`
	VoiceVlan       *int16  `json:"voice_vlan"`
	VoiceVlanName   *string `json:"voice_vlan_name"`
	CollectedAt     string  `json:"collected_at"`
}
