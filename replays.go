package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/fatih/color"
)

func opCleanReplays() {
	dir := removeTrailingSlash(c.RippleDir) + "/osu.ppy.sh/replays"
	if finfo, err := os.Stat(dir); err != nil || !finfo.IsDir() {
		color.Red("> CleanReplays: failed to start cleaning replays:")
		if err != nil {
			color.Red("> %v", err)
		} else {
			color.Red("> %s is a file, not a folder", dir)
		}
		return
	}

	const failedReplays = "SELECT id FROM scores WHERE completed != 3"
	rows, err := db.Query(failedReplays)
	if err != nil {
		queryError(err, failedReplays)
	}
	count := 0
	for rows.Next() {
		if count%50 == 0 && count != 0 {
			fmt.Println("> CleanReplays:", count, "replays cleared")
		}
		var scoreID int
		err := rows.Scan(&scoreID)
		if err != nil {
			queryError(err, failedReplays)
			continue
		}
		filename := fmt.Sprintf("%s/replays/replay_%d.osr", dir, scoreID)
		// We don't check if the file exists, because that would be an useless I/O operation
		// TODO: WorkGroup?
		os.Remove(filename)
		count++
	}
	rows.Close()
	color.Green("> CleanReplays: done!")
	wg.Done()
}
func removeTrailingSlash(s string) string {
	if s[len(s)-1] == '/' {
		return s[:len(s)-1]
	}
	return s
}

func opDeleteReplayCache() {
	dir := removeTrailingSlash(c.RippleDir) + "/osu.ppy.sh/replays_full"
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		color.Red("> DeleteReplaysFull: Couldn't get files from replays_full directory: %v", err)
		return
	}

	count := 0
	for _, file := range files {
		err := os.Remove(dir + "/" + file.Name())
		if err != nil {
			color.Red("> DeleteReplaysFull: couldn't remove file %s: %v", file.Name(), err)
			continue
		}
		count++
	}
	color.Green("> DeleteReplaysFull: done! %d replays deleted", count)

	wg.Done()
}
