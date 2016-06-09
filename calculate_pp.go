package main

import (
	"fmt"
	"math"

	"github.com/fatih/color"
)

type ppUserMode struct {
	countScores int
	ppTotal     int
}

func opCalculatePP() {
	defer wg.Done()

	const ppQuery = "SELECT scores.userid, pp, scores.play_mode FROM scores LEFT JOIN users ON users.id=scores.userid WHERE completed = '3' AND users.allowed = '1' ORDER BY pp DESC"
	rows, err := db.Query(ppQuery)
	if err != nil {
		queryError(err, ppQuery)
		return
	}

	users := make(map[int]*ppUserMode)
	var count int

	for rows.Next() {
		if count%1000 == 0 {
			fmt.Println("> CalculatePP:", count)
		}
		var (
			userid   int
			ppAmt    float64
			playMode int
		)
		err := rows.Scan(&userid, &ppAmt, &playMode)
		if err != nil {
			queryError(err, ppQuery)
			continue
		}
		if users[userid] == nil {
			users[userid] = &ppUserMode{}
		}
		if users[userid].countScores > 100 {
			continue
		}
		currentScorePP := round(round(ppAmt) * math.Pow(0.95, float64(users[userid].countScores)))
		users[userid].countScores++
		users[userid].ppTotal += int(currentScorePP)
		count++
	}
	rows.Close()

	for userid, ppUM := range users {
		op("UPDATE users_stats SET pp_std = ? WHERE id = ?", ppUM.ppTotal, userid)
	}

	color.Green("> CalculatePP: done!")

	if c.BuildLeaderboards {
		fmt.Print("Starting building leaderboards...")
		go opBuildLeaderboard()
		color.Green(" ok!")
	}
}

func round(a float64) float64 {
	if a < 0 {
		return math.Ceil(a - 0.5)
	}
	return math.Floor(a + 0.5)
}
