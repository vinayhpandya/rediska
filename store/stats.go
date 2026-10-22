package store

// KeyspaceStat holds the counters INFO keyspace reports, one map per
// database (only database 0 is used). Only "keys" is kept up to date so
// far; "expires" and "avg_ttl" are read by INFO but always 0.
var KeyspaceStat [4]map[string]int

func init() {
	for i := range KeyspaceStat {
		KeyspaceStat[i] = make(map[string]int)
	}
}

// UpdateDbStat adds value (which may be negative) to the counter metric of
// database num.
func UpdateDbStat(num int, metric string, value int) {
	KeyspaceStat[num][metric] += value
}
