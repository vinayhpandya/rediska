package store

var KeyspaceStat [4]map[string]int

func init() {
	for i := range KeyspaceStat {
		KeyspaceStat[i] = make(map[string]int)
	}
}
func UpdateDbStat(num int, metric string, value int) {
	KeyspaceStat[num][metric] += value
}
