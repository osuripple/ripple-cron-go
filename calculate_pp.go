package main

import (
	"math"

	"github.com/fatih/color"
)

type ppUserMode struct {
	countScores int
	ppTotal     int
}

func opCalculatePP() {
	defer wg.Done()

	const ppQuery = "SELECT scores.userid, pp, scores.play_mode, scores.is_relax FROM scores INNER JOIN users ON users.id=scores.userid JOIN beatmaps USING(beatmap_md5) WHERE completed = 3 AND ranked >= 2 AND disable_pp = 0 AND pp IS NOT NULL ORDER BY pp DESC LIMIT 10000"
	rows, err := db.Query(ppQuery)
	if err != nil {
		queryError(err, ppQuery)
		return
	}

	users := make(map[int]*[2]*[4]*ppUserMode)
	var count int

	for rows.Next() {
		if count%1000 == 0 {
			verboseln("> CalculatePP:", count)
		}
		var (
			userid   int
			ppAmt    *float64
			playMode int
			isRelax  int8
		)
		err := rows.Scan(&userid, &ppAmt, &playMode, &isRelax)
		if err != nil {
			queryError(err, ppQuery)
			continue
		}
		if ppAmt == nil {
			continue
		}
		if users[userid] == nil {
			var arr [2]*[4]*ppUserMode
			for relax := 0; relax < 1; relax++ {
				arr[relax] = &[4]*ppUserMode{
					new(ppUserMode),
					new(ppUserMode),
					new(ppUserMode),
					new(ppUserMode),
				}
			}
			users[userid] = &arr
		}
		if users[userid][isRelax][playMode].countScores > 500 {
			continue
		}
		currentScorePP := round(round(*ppAmt) * math.Pow(0.95, float64(users[userid][isRelax][playMode].countScores)))
		users[userid][isRelax][playMode].countScores++
		users[userid][isRelax][playMode].ppTotal += int(currentScorePP)
		count++
	}
	rows.Close()

	var table string
	for userid, ppr := range users {
		for relax, pps := range *ppr {
			if pps == nil {
				continue
			}
			if relax == 0 {
				table = "users_stats"
			} else {
				table = "users_stats_relax"
			}
			for mode, ppUM := range *pps {
				op("UPDATE "+table+" SET pp_"+modeToString(mode)+" = ? WHERE id = ? LIMIT 1", ppUM.ppTotal, userid)
			}
		}
	}

	color.Green("> CalculatePP: done!")

	if c.PopulateRedis {
		verboseln("Starting to populate redis")
		go opPopulateRedis()
	}
}

func round(a float64) float64 {
	if a < 0 {
		return math.Ceil(a - 0.5)
	}
	return math.Floor(a + 0.5)
}
