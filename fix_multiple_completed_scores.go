package main

import (
	"fmt"

	"github.com/fatih/color"
)

func opFixMultipleCompletedScores() {
	defer wg.Done()
	const initQuery = "SELECT id, userid, beatmap_md5, play_mode, score FROM scores WHERE completed = 3 ORDER BY id DESC"
	scores := []score{}
	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}
	for rows.Next() {
		currentScore := score{}
		rows.Scan(
			&currentScore.id,
			&currentScore.userid,
			&currentScore.beatmapMD5,
			&currentScore.score,
			&currentScore.playMode,
		)
		scores = append(scores, currentScore)
	}
	fmt.Println("> FixMultipleCompletedScores: Fetched, now finding bugged completed scores...")

	var ops int64
	fixed := []int{}
	for i := 0; i < len(scores); i++ {
		ops++
		if ops%1000 == 0 {
			fmt.Println("> FixMultipleCompletedScores:", ops)
		}
		if contains(fixed, scores[i].id) {
			continue
		}
		for j := i + 1; j < len(scores); j++ {
			if contains(fixed, scores[j].id) {
				continue
			}
			if (scores[j].id != scores[i].id && scores[j].beatmapMD5 == scores[i].beatmapMD5 && scores[j].userid == scores[i].userid && scores[j].playMode == scores[i].playMode) {
				fmt.Printf("> FixMultipleCompletedScores: Found duplicated completed score (%d/%d)\n", scores[i].id, duplicate.id)
				if (scores[j].score > scores[i].score) {
					op("UPDATE scores SET completed = 2 WHERE id = ?", scores[i].id)
				} else {
					op("UPDATE scores SET completed = 2 WHERE id = ?", scores[j].id)
				}
				fixed = append(fixed, scores[i].id, scores[j].id)
			}
		}
	}

	color.Green("> FixMultipleCompletedScores: done!")
}
