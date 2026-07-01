package vlans

type VlanRecord struct {
	VlanID int32   `json:"vlan_id"`
	Name   *string `json:"name"`
}

type VlansResponse struct {
	Vlans []VlanRecord `json:"vlans"`
}
