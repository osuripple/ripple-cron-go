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
	const ppQuery = "SELECT scores.username, pp, scores.play_mode FROM scores LEFT JOIN users ON users.username=scores.username WHERE completed = '3' AND users.allowed = '1' ORDER BY pp DESC"
	rows, err := db.Query(ppQuery)
	if err != nil {
		queryError(err, ppQuery)
		return
	}

	users := make(map[string]*ppUserMode)
	var count int

	for rows.Next() {
		if count%1000 == 0 {
			fmt.Println("> CalculatePP:", count)
		}
		var (
			username string
			ppAmt    float64
			playMode int
		)
		err := rows.Scan(&username, &ppAmt, &playMode)
		if err != nil {
			queryError(err, ppQuery)
			continue
		}
		if users[username] == nil {
			users[username] = &ppUserMode{}
		}
		currentScorePP := math.Ceil(math.Ceil(ppAmt) * math.Pow(0.95, float64(users[username].countScores)))
		users[username].countScores++
		users[username].ppTotal += int(currentScorePP)
		count++
	}
	rows.Close()

	for username, ppUM := range users {
		op("UPDATE users_stats SET pp_std = ? WHERE username = ?", ppUM.ppTotal, username)
	}

	color.Green("> CalculatePP: done!")

	wg.Done()

	if c.BuildLeaderboards {
		fmt.Print("Starting building leaderboards...")
		go opBuildLeaderboard()
		color.Green(" ok!")
	}
}
