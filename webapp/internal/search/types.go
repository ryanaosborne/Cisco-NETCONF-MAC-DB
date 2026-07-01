package search

// Result is one row returned by the search query.
type Result struct {
	NodeID               string  `json:"node_id"`
	MacAddress           string  `json:"mac_address"`
	IPAddress            *string `json:"ip_address"`
	Interface            *string `json:"interface"`
	InterfaceDescription *string `json:"interface_description"`
	AccessVlan           *int32  `json:"access_vlan"`
	AccessVlanName       *string `json:"access_vlan_name"`
	VoiceVlan            *int32  `json:"voice_vlan"`
	VoiceVlanName        *string `json:"voice_vlan_name"`
}

// SearchResponse is the JSON envelope returned by POST /api/search.
type SearchResponse struct {
	Results  []Result `json:"results"`
	NotFound []string `json:"not_found"`
}

// termRecord tracks each unique input term and what normalised values to look
// for in query results so we can report which terms had no match.
type termRecord struct {
	original string
	kind     string   // "mac", "ip", "hostname"
	lookFor  []string // normalised MAC forms, resolved IPs, or bare IP
}
