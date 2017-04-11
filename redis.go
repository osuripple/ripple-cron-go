package main

import (
	"strings"

	"github.com/fatih/color"
	redis "gopkg.in/redis.v5"
)

func opPopulateRedis() {
	defer wg.Done()

	s, err := r.Keys("ripple:leaderboard:*").Result()
	if err != nil {
		color.Red("> PopulateRedis: %v", err)
		return
	}

	if len(s) > 0 {
		err = r.Eval("return redis.call('del', unpack(redis.call('keys', 'ripple:leaderboard:*')))", nil).Err()
		if err != nil {
			color.Red("> PopulateRedis: %v", err)
			return
		}
	}

	const initQuery = "SELECT users_stats.id, users_stats.country, pp_std, ranked_score_taiko, ranked_score_ctb, pp_mania FROM users_stats INNER JOIN users ON users.id = users_stats.id WHERE privileges & 1 > 0"

	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}

	var (
		uid     int
		country string
		pp      [4]int64
	)
	for rows.Next() {
		err = rows.Scan(&uid, &country, &pp[0], &pp[1], &pp[2], &pp[3])
		if err != nil {
			queryError(err, initQuery)
			continue
		}

		for k, v := range pp {
			r.ZAdd("ripple:leaderboard:"+modes[k], redis.Z{
				Member: uid,
				Score:  float64(v),
			})
			r.ZAdd("ripple:leaderboard:"+modes[k]+":"+strings.ToLower(country), redis.Z{
				Member: uid,
				Score:  float64(v),
			})
		}
	}

	color.Green("> PopulateRedis: done!")
}
