package main

import (
	"fmt"
	"math"
	"sort"

	"github.com/fatih/color"
)

// CLUSTERFUCK AHEAD DO NOT TOUCH

type calculateOverallAccuracyElement struct {
	mode     int
	accuracy float64
	pp       float64
}

func (c calculateOverallAccuracyElement) g() float64 {
	if c.mode == 0 {
		return c.pp
	}
	return c.accuracy
}

type coaeCollection []calculateOverallAccuracyElement

func (c coaeCollection) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c coaeCollection) Less(i, j int) bool {
	return c[i].g() > c[j].g()
}
func (c coaeCollection) Len() int {
	return len(c)
}

// https://git.zxq.co/ripple/old-frontend/src/70faf80910742c9418ed4e4a13a622a6d7cca9cb/inc/functions.php#L1293-L1319
func (c coaeCollection) Weighten() float64 {
	var total float64
	var divideTotal float64
	for i, el := range c {
		add := math.Pow(0.95, float64(i)) * 100
		total += el.accuracy * add
		divideTotal += add
	}
	if divideTotal == 0 {
		return 0
	}
	return total / divideTotal
}

type coaeCollectionCollection [4]coaeCollection

func (c *coaeCollectionCollection) Add(n calculateOverallAccuracyElement) error {
	if c == nil {
		return fmt.Errorf("fuck")
	}
	c[n.mode] = append(c[n.mode], n)
	return nil
}

func opCalculateOverallAccuracy() {
	defer wg.Done()
	data := make(map[int]*coaeCollectionCollection)
	const memeQuery = "SELECT users.id, scores.play_mode, scores.accuracy, scores.pp FROM scores INNER JOIN users ON users.id = scores.userid WHERE completed = '3'"
	rows, err := db.Query(memeQuery)
	if err != nil {
		queryError(err, memeQuery)
		return
	}
	for rows.Next() {
		var (
			uid int
			el  calculateOverallAccuracyElement
		)
		err = rows.Scan(&uid, &el.mode, &el.accuracy, &el.pp)
		if err != nil {
			queryError(err, memeQuery)
			continue
		}
		// silently ignore invalid modes
		if el.mode < 0 || el.mode > 3 {
			continue
		}
		if data[uid] == nil {
			data[uid] = new(coaeCollectionCollection)
		}
		data[uid].Add(el)
	}

	for _, v := range data {
		// VARIABLE SHADOWING FTW
		for _, v := range v {
			sort.Sort(v)
		}
	}

	for userid, info := range data {
		for mode, scores := range info {
			op("UPDATE users_stats SET avg_accuracy_"+modes[mode]+" = ? WHERE id = ?", scores.Weighten(), userid)
		}
	}

	color.Green("> CalculateOverallAccuracy: done!")
}
