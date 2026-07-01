package search

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	MacRe  = regexp.MustCompile(`(?i)^([0-9a-f]{2}[:\-]){5}[0-9a-f]{2}$|^[0-9a-f]{4}\.[0-9a-f]{4}\.[0-9a-f]{4}$|^[0-9a-f]{12}$`)
	IPRe   = regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}$`)
	HostRe = regexp.MustCompile(`(?i)^[a-z][a-z0-9\-.]*$`)
)

// normalizeMac returns both colon and Cisco-dot forms of a MAC so the query
// matches regardless of which format the device wrote to the database.
func normalizeMac(s string) []string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			b.WriteRune(r)
		}
	}
	if b.Len() != 12 {
		return []string{strings.ToLower(s)}
	}
	h := b.String()
	colon := fmt.Sprintf("%s:%s:%s:%s:%s:%s", h[0:2], h[2:4], h[4:6], h[6:8], h[8:10], h[10:12])
	dot := fmt.Sprintf("%s.%s.%s", h[0:4], h[4:8], h[8:12])
	return []string{colon, dot}
}

// searchSQL finds rows by MAC or IP address.
// latest_arp keeps only the newest ARP entry per MAC so stale duplicates
// (e.g. a device that moved ports) do not produce multiple result rows.
// The UNION second half catches IPs whose MAC hasn't yet appeared in mac_table.
const searchSQL = `
WITH latest_arp AS (
    SELECT DISTINCT ON (mac_address)
        node_id, ip_address, mac_address, interface, age_seconds, collected_at
    FROM arp_table
    ORDER BY mac_address, collected_at DESC
),
best_mac AS (
    -- When the same MAC is learned by multiple switches (e.g. via a trunk/port-channel
    -- upstream), prefer the physical access port entry over logical interfaces.
    -- Ties on rank fall back to most-recent collected_at.
    SELECT DISTINCT ON (mac_address)
        node_id, mac_address, interface, collected_at
    FROM mac_table
    ORDER BY mac_address,
        CASE
            WHEN interface ILIKE 'Vlan%'         THEN 3
            WHEN interface ILIKE 'Loopback%'      THEN 3
            WHEN interface ILIKE 'Tunnel%'        THEN 3
            WHEN interface ILIKE 'Port-channel%'  THEN 2
            ELSE 1
        END ASC,
        collected_at DESC
)
SELECT
    m.node_id,
    m.mac_address,
    a.ip_address,
    m.interface,
    i.description  AS interface_description,
    i.access_vlan,
    dv.name        AS access_vlan_name,
    i.voice_vlan,
    vv.name        AS voice_vlan_name
FROM best_mac m
LEFT JOIN latest_arp      a  ON  m.mac_address  = a.mac_address
LEFT JOIN interface_table i  ON  m.node_id      = i.node_id AND m.interface   = i.name
LEFT JOIN vlan_table      dv ON  m.node_id      = dv.node_id AND i.access_vlan = dv.vlan_id
LEFT JOIN vlan_table      vv ON  m.node_id      = vv.node_id AND i.voice_vlan  = vv.vlan_id
WHERE lower(m.mac_address) = ANY($1) OR a.ip_address = ANY($2)

UNION

SELECT
    a.node_id,
    COALESCE(a.mac_address, ''),
    a.ip_address,
    NULL::text,
    NULL::text,
    NULL::smallint,
    NULL::text,
    NULL::smallint,
    NULL::text
FROM (
    SELECT DISTINCT ON (ip_address)
        node_id, ip_address, mac_address, collected_at
    FROM arp_table
    WHERE ip_address = ANY($2)
    ORDER BY ip_address, collected_at DESC
) a
WHERE NOT EXISTS (
    SELECT 1 FROM mac_table WHERE lower(mac_address) = lower(a.mac_address)
)

ORDER BY node_id, mac_address
`
