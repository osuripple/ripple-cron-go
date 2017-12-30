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

	const initQuery = `
SELECT
	users_stats.id, users_stats.country, pp_std,
	pp_taiko, pp_ctb, pp_mania,
	playcount_std, playcount_taiko, playcount_ctb, playcount_mania,
	users.latest_activity
FROM users_stats INNER JOIN users ON users.id = users_stats.id WHERE privileges & 1 > 0`

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
			r.ZAdd("ripple:leaderboard:"+modes[k], redis.Z{
				Member: uid,
				Score:  float64(v),
			})
			if country != "xx" && country != "" {
				r.ZAdd("ripple:leaderboard:"+modes[k]+":"+country, redis.Z{
					Member: uid,
					Score:  float64(v),
				})
			}
		}
	}

	color.Green("> PopulateRedis: done!")
}

func isInactive(secondsInactive float64, playcount int) bool {
	daysInactive := secondsInactive / (60 * 60 * 24)
	return daysInactive > (math.Log(float64(playcount)) * 16)
}
