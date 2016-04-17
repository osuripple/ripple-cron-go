package main

import (
	"fmt"
	"math"
)

func opCalculateAccuracy() {
	const initQuery = "SELECT id, 300_count, 100_count, 50_count, gekis_count, katus_count, misses_count, play_mode, accuracy FROM scores"
	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
	}
	count := 0
	for rows.Next() {
		if count%1000 == 0 {
			fmt.Println("> CalculateAccuracy:", count)
		}
		var (
			id        int
			count300  int
			count100  int
			count50   int
			countgeki int
			countkatu int
			countmiss int
			playMode  int
			accuracy  *float64
		)
		err := rows.Scan(&id, &count300, &count100, &count50, &countgeki, &countkatu, &countmiss, &playMode, &accuracy)
		if err != nil {
			queryError(err, initQuery)
			continue
		}
		if accuracy == nil {
			var a float64
			accuracy = &a
		}
		newAcc := calculateAccuracy(count300, count100, count50, countgeki, countkatu, countmiss, playMode)
		// if accuracies are not accurate to the .001
		if !math.IsNaN(newAcc) && math.Floor(newAcc*1000) != math.Floor((*accuracy)*1000) {
			op("UPDATE scores SET accuracy = ? WHERE id = ?", newAcc, id)
		}
		count++
	}
	rows.Close()
	wg.Done()
}

func calculateAccuracy(count300, count100, count50, countgeki, countkatu, countmiss, playMode int) float64 {
	var accuracy float64
	switch playMode {
	case 1:
		// Please note this is not what is written on the wiki.
		// However, what was written on the wiki didn't make any sense at all.
		totalPoints := (count100*50 + count300*100)
		maxHits := (countmiss + count100 + count300)
		accuracy = float64(totalPoints) / float64(maxHits*100)
	case 2:
		fruits := count300 + count100 + count50
		totalFruits := fruits + countmiss + countkatu
		accuracy = float64(fruits) / float64(totalFruits)
	case 3:
		totalPoints := (count50*50 + count100*100 + countkatu*200 + count300*300 + countgeki*300)
		maxHits := (countmiss + count50 + count100 + count300 + countgeki + countkatu)
		accuracy = float64(totalPoints) / float64(maxHits*300)
	default:
		totalPoints := (count50*50 + count100*100 + count300*300)
		maxHits := (countmiss + count50 + count100 + count300)
		accuracy = float64(totalPoints) / float64(maxHits*300)
	}
	return accuracy * 100
}
