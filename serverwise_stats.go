package main

import (
	"github.com/fatih/color"
)

type statType struct {
	query    string
	redisKey string
	value    int
}

func opServerwiseStats() {
	defer wg.Done()

	stats := []statType {
		statType {
			query: "SELECT COUNT(id) AS c FROM scores LIMIT 1",
			redisKey: "ripple:total_submitted_scores",
		},
		statType {
			query: "SELECT SUM(playcount_std) + SUM(playcount_taiko) + SUM(playcount_ctb) + SUM(playcount_mania) FROM users_stats WHERE 1",
			redisKey: "ripple:total_plays",
		},
		statType {
			query: "SELECT SUM(pp_std) + SUM(pp_taiko) + SUM(pp_ctb) + SUM(pp_mania) AS s FROM users_stats WHERE 1 LIMIT 1",
			redisKey: "ripple:total_pp",
		},
	}
	for _, v := range stats {
		db.QueryRow(v.query).Scan(&v.value)
		r.Set(v.redisKey, v.value, 0)
	}


	color.Green("> Server-wise Stats: done!")
}