package main

import (
	"math"
	"strings"
	"time"

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

	r.Del("hanayo:country_list")

	populateLeaderboard(false)
	populateLeaderboard(true)

	color.Green("> PopulateRedis: done!")
}

func populateLeaderboard(relax bool) {
	var table string
	var suffix string
	if relax {
		table = "users_stats_relax"
		suffix = ":relax"
	} else {
		table = "users_stats"
		suffix = ""
	}
	initQuery := `
SELECT
	users_stats.id, full_stats.country, users_stats.pp_std,
	users_stats.pp_taiko, users_stats.pp_ctb, users_stats.pp_mania,
	users_stats.playcount_std, users_stats.playcount_taiko, users_stats.playcount_ctb, users_stats.playcount_mania,
	users.latest_activity
FROM ` + table + ` AS users_stats JOIN users_stats AS full_stats USING(id) INNER JOIN users USING(id) WHERE is_public = 1`

	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}

	currentSeconds := time.Now().Unix()

	var (
		uid            int
		country        string
		pp             [4]int64
		playcount      [4]int
		latestActivity int64
	)
	for rows.Next() {
		err = rows.Scan(
			&uid, &country, &pp[0],
			&pp[1], &pp[2], &pp[3],
			&playcount[0], &playcount[1], &playcount[2], &playcount[3],
			&latestActivity,
		)
		if err != nil {
			queryError(err, initQuery)
			continue
		}

		country = strings.ToLower(country)

		if country != "xx" && country != "" {
			r.ZIncrBy("hanayo:country_list", 1, country)
		}

		for k, v := range pp {
			if isInactive(float64(currentSeconds-latestActivity), playcount[k]) {
				continue
			}
			r.ZAdd("ripple:leaderboard:"+modes[k]+suffix, redis.Z{
				Member: uid,
				Score:  float64(v),
			})
			if country != "xx" && country != "" {
				r.ZAdd("ripple:leaderboard:"+modes[k]+":"+country+suffix, redis.Z{
					Member: uid,
					Score:  float64(v),
				})
			}
		}
	}
}

func isInactive(secondsInactive float64, playcount int) bool {
	daysInactive := secondsInactive / (60 * 60 * 24)
	return daysInactive > (math.Log(float64(playcount)) * 16)
}
