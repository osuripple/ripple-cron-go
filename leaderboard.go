package main

import (
	"fmt"
	"sort"

	"github.com/fatih/color"
)

type lbUser struct {
	uid int
	pp  int
}

type lbUserSlice []lbUser

// functions to satisfy sort.Interface

func (l lbUserSlice) Len() int {
	return len(l)
}
func (l lbUserSlice) Less(i, j int) bool {
	return l[i].pp > l[j].pp
}
func (l lbUserSlice) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func opBuildLeaderboard() {
	initQuery := "SELECT users_stats.id, pp_std, pp_taiko, pp_ctb, pp_mania FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users.allowed = '1'"
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
			uid     int
			stdPP   int
			taikoPP int
			ctbPP   int
			maniaPP int
		)
		rows.Scan(&uid, &stdPP, &taikoPP, &ctbPP, &maniaPP)
		std = append(std, lbUser{uid, stdPP})
		taiko = append(taiko, lbUser{uid, taikoPP})
		ctb = append(ctb, lbUser{uid, ctbPP})
		mania = append(mania, lbUser{uid, maniaPP})
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
			params = append(params, pos+1, us.uid, us.pp)
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
