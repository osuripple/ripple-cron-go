package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/fatih/color"
)

func opCleanReplays() {
	defer wg.Done()

	if c.ReplayFolder == "" {
		return
	}

	// we're using os.Open instead of ioutil.Readdir
	// so that we can take advantage of Readdirnames (which uses far less
	// memory)
	dir, err := os.Open(c.ReplayFolder)
	if err != nil {
		color.Red("> CleanReplays: can't read dir %v", err)
		return
	}

	names, err := dir.Readdirnames(-1)
	dir.Close()
	if err != nil {
		color.Red("> CleanReplays: can't read names of dir %v", err)
		return
	}

	repsFolder := replaysToIntSlice(names)

	// get ids of all scores in database
	var repsDB []int
	const scoresQuery = "SELECT id FROM scores WHERE completed = 3"
	err = db.Select(&repsDB, scoresQuery)
	if err != nil {
		queryError(err, scoresQuery)
		return
	}

	// remove from repsFolder all replays
	for _, i := range repsDB {
		for pos, j := range repsFolder {
			if i == j {
				repsFolder[pos] = repsFolder[len(repsFolder)-1]
				repsFolder = repsFolder[:len(repsFolder)-1]
				break
			}
		}
	}

	color.Green("> CleanReplays: done!")
}

func replaysToIntSlice(replays []string) []int {
	i := make([]int, 0, len(replays))
	var j int
	var err error
	for _, r := range replays {
		j, err = strconv.Atoi(strings.TrimPrefix(strings.TrimSuffix(r, ".osr"), "replay_"))
		if err != nil {
			continue
		}
		i = append(i, j)
	}
	return i
}
