package main

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/fatih/color"
)

type lbUser struct {
	uid int
	pp  int64
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
	defer wg.Done()
	// creating a new db instance so that we don't have to execute everything
	// in 1 mysql worker for the main stuff
	db, err := sql.Open("mysql", c.DSN)
	if err != nil {
		color.Red("> BuildLeaderboard: couldn't start secondary db connection (%v)", err)
		return
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	const initQuery = "SELECT users_stats.id, pp_std, ranked_score_taiko, ranked_score_ctb, pp_mania FROM users_stats INNER JOIN users ON users.id = users_stats.id WHERE privileges & 1 > 0"
	logquery(initQuery, nil)
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
			stdPP   int64
			taikoPP int64
			ctbPP   int64
			maniaPP int64
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
		q := "LOCK TABLES leaderboard_" + modeToString(modeID) + " WRITE"
		logquery(q, nil)
		_, err := db.Exec(q)
		if err != nil {
			queryError(err, q)
			return
		}

		q = "TRUNCATE TABLE leaderboard_" + modeToString(modeID)
		logquery(q, nil)
		_, err = db.Exec(q)
		if err != nil {
			queryError(err, q)
		}
		logquery(modeInsert, params)
		_, err = db.Exec(modeInsert, params...)
		if err != nil {
			queryError(err, "<modeInsert>")
		}

		_, err = db.Exec("UNLOCK TABLES")
		logquery("UNLOCK TABLES", nil)
		if err != nil {
			queryError(err, "UNLOCK TABLES")
			return
		}

		color.Green("> BuildLeaderboard: %s leaderboard built!", modeToString(modeID))
	}
}
