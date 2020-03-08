package main

import (
	"container/heap"
	"math"

	"github.com/fatih/color"
)

// Float64Heap is a max heap of float64s
type Float64Heap []float64

func (h Float64Heap) Len() int           { return len(h) }
func (h Float64Heap) Less(i, j int) bool { return h[i] > h[j] }
func (h Float64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push golint pls stop
func (h *Float64Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(float64))
}

// Pop golint pls stop
func (h *Float64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func opCalculatePP() {
	defer wg.Done()

	// We do not rely on MySQL to sort the scores by pp or
	// the scores table will be locked for a (very) long time,
	// so we fetch the scores in an arbitrary order and we
	// let the cron sort them by pp.
	const ppQuery = "SELECT scores.userid, pp, scores.play_mode, scores.is_relax FROM scores JOIN users ON users.id=scores.userid JOIN beatmaps USING(beatmap_md5) WHERE completed = 3 AND ranked >= 2 AND disable_pp = 0"
	rows, err := db.Query(ppQuery)
	if err != nil {
		queryError(err, ppQuery)
		return
	}

	var count int
	users := make(map[int]*[2]*[4]*Float64Heap)
	for rows.Next() {
		if count%100000 == 0 {
			verboseln("> CalculatePP: fetched", count)
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
			var arr [2]*[4]*Float64Heap
			for relax := 0; relax <= 1; relax++ {
				arr[relax] = &[4]*Float64Heap{
					new(Float64Heap),
					new(Float64Heap),
					new(Float64Heap),
					new(Float64Heap),
				}
			}
			users[userid] = &arr
		}
		heap.Push(users[userid][isRelax][playMode], *ppAmt)
		count++
	}
	rows.Close()
	verboseln("> CalculatePP: everything fetched and sorted. Now calculating sum of weighted pp.")
	count = 0
	for userID, relaxData := range users {
		for isRelax, gameModeData := range relaxData {
			for gameMode, ppData := range gameModeData {
				var totalPP float64
				heapSize := ppData.Len()
				if heapSize > 500 {
					heapSize = 500
				}

				// Calculate sum of weighted pp for the top 500 scores
				for i := 0; i < heapSize; i++ {
					count++
					if count%100000 == 0 {
						verboseln("> CalculatePP:", count)
					}
					pp := heap.Pop(ppData).(float64)
					totalPP += round(round(pp) * math.Pow(0.95, float64(i)))
				}

				// Calculated, now update in db
				var table string
				if isRelax == 0 {
					table = "users_stats"
				} else {
					table = "users_stats_relax"
				}
				op("UPDATE "+table+" SET pp_"+modeToString(gameMode)+" = ? WHERE id = ? LIMIT 1", totalPP, userID)
				// verboseln("> Weighted pp for", userID, isRelax, gameMode, "=", totalPP)
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
