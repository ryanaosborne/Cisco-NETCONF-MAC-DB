package portlookup

// LookupRequest is the JSON body for POST /api/portlookup.
type LookupRequest struct {
	Switch       string   `json:"switch"`       // partial switch name (ILIKE match)
	Descriptions []string `json:"descriptions"` // partial port descriptions (ILIKE match each)
}

// PortMatch is one row returned by the lookup query.
type PortMatch struct {
	NodeID         string  `json:"node_id"`
	Interface      string  `json:"interface"`
	Description    *string `json:"description"`
	AccessVlan     *int16  `json:"access_vlan"`
	AccessVlanName *string `json:"access_vlan_name"`
	VoiceVlan      *int16  `json:"voice_vlan"`
	VoiceVlanName  *string `json:"voice_vlan_name"`
}

// LookupResponse is the JSON envelope returned by POST /api/portlookup.
type LookupResponse struct {
	Results []PortMatch `json:"results"`
}
