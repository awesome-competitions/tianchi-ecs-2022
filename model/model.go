package model

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ZEntry struct {
	Score int    `json:"score"`
	Value string `json:"value"`
}

type ZRangeEntry struct {
	MinScore int `json:"min_score"`
	MaxScore int `json:"max_score"`
}

type UpdateCluster struct {
	Hosts []string `json:"hosts"`
	Index int      `json:"index"`
}
