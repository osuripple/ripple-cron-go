package main

import (
	"fmt"
	"sort"

	"github.com/fatih/color"
)

type lbUser struct {
	uid   int
	score int64
}

type lbUserSlice []lbUser

// functions to satisfy sort.Interface

func (l lbUserSlice) Len() int {
	return len(l)
}
func (l lbUserSlice) Less(i, j int) bool {
	return l[i].score > l[j].score
}
func (l lbUserSlice) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func opBuildLeaderboard() {
	initQuery := "SELECT users_stats.id, ranked_score_std, ranked_score_taiko, ranked_score_ctb, ranked_score_mania FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users.allowed = '1'"
	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}
	var (
		std   lbUserSlice
		taiko lbUserSlice
		ctb   lbUserSlice
		mania lbUserSlice
	)
	for rows.Next() {
		var (
			uid        int
			stdScore   int64
			taikoScore int64
			ctbScore   int64
			maniaScore int64
		)
		rows.Scan(&uid, &stdScore, &taikoScore, &ctbScore, &maniaScore)
		std = append(std, lbUser{uid, stdScore})
		taiko = append(taiko, lbUser{uid, taikoScore})
		ctb = append(ctb, lbUser{uid, ctbScore})
		mania = append(mania, lbUser{uid, maniaScore})
	}
	rows.Close()
	sort.Sort(std)
	sort.Sort(taiko)
	sort.Sort(ctb)
	sort.Sort(mania)
	for modeID, sl := range []lbUserSlice{std, taiko, ctb, mania} {
		if len(sl) < 1 {
			continue
		}
		var params []interface{}
		var modeInsert = "INSERT INTO leaderboard_" + modeToString(modeID) + " (position, user, v) VALUES "
		for pos, us := range sl {
			modeInsert += fmt.Sprintf("(?, ?, ?), ")
			params = append(params, pos+1, us.uid, us.score)
		}
		// remove last two characters (`, `)
		modeInsert = modeInsert[:len(modeInsert)-2]
		// In this case, it is really important to truncate BEFORE and then add the insert.
		_, err := db.Exec("TRUNCATE TABLE leaderboard_" + modeToString(modeID))
		if err != nil {
			queryError(err, "TRUNCATE TABLE leaderboard_"+modeToString(modeID))
		}
		op(modeInsert, params...)
		color.Green("> BuildLeaderboard: %s leaderboard built!", modeToString(modeID))
	}
	wg.Done()
}
