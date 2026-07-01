package dbview

import "time"

type macRow struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	MacAddress  string    `json:"mac_address"`
	Interface   *string   `json:"interface"`
	Vlan        *int32    `json:"vlan"`
	MacType     *string   `json:"mac_type"`
	CollectedAt time.Time `json:"collected_at"`
}

type arpRow struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	IPAddress   string    `json:"ip_address"`
	MacAddress  *string   `json:"mac_address"`
	Interface   *string   `json:"interface"`
	AgeSeconds  *int32    `json:"age_seconds"`
	CollectedAt time.Time `json:"collected_at"`
}

type interfaceRow struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	Shutdown    bool      `json:"shutdown"`
	IPAddress   *string   `json:"ip_address"`
	PrefixLen   *int16    `json:"prefix_len"`
	VRF         *string   `json:"vrf"`
	MTU         *int32    `json:"mtu"`
	AccessVlan  *int16    `json:"access_vlan"`
	VoiceVlan   *int16    `json:"voice_vlan"`
	CollectedAt time.Time `json:"collected_at"`
}

type vlanRow struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	VlanID      int32     `json:"vlan_id"`
	Name        *string   `json:"name"`
	Status      *string   `json:"status"`
	CollectedAt time.Time `json:"collected_at"`
}

type dbInspectResponse struct {
	MacRows        []macRow       `json:"mac_rows"`
	MacTotal       int            `json:"mac_total"`
	ArpRows        []arpRow       `json:"arp_rows"`
	ArpTotal       int            `json:"arp_total"`
	InterfaceRows  []interfaceRow `json:"interface_rows"`
	InterfaceTotal int            `json:"interface_total"`
	VlanRows       []vlanRow      `json:"vlan_rows"`
	VlanTotal      int            `json:"vlan_total"`
}

type tableResp struct {
	Rows          interface{} `json:"rows"`
	Total         int         `json:"total"`
	FilteredTotal int         `json:"filtered_total"`
	Page          int         `json:"page"`
	PageSize      int         `json:"page_size"`
	Pages         int         `json:"pages"`
}
