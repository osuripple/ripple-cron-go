package main

import (
	"io/ioutil"
	"os"

	"github.com/fatih/color"
)

func opClearExpiredProfileBackgrounds() {
	defer wg.Done()

	if c.HanayoFolder == "" {
		color.Red("> ClearExpiredProfileBackgrounds: HanayoFolder is empty. ignoring")
		return
	}

	// get all the backgrounds
	elementsRaw, err := ioutil.ReadDir(c.HanayoFolder + "/static/profbackgrounds")
	if err != nil {
		color.Red("> ClearExpiredProfileBackgrounds: failed to get profile backgrounds: %v", err)
		return
	}

	// convert to []string
	elements := make([]string, len(elementsRaw))
	for i, v := range elementsRaw {
		if v.Name() == ".keep" {
			continue
		}
		elements[i] = v.Name()
	}

	// get profile backgrounds in db
	const q = "SELECT uid FROM profile_backgrounds WHERE type = 1"
	inDB, err := db.Query(q)
	if err != nil {
		queryError(err, q)
	}

	// remove from elements every background that does actually exist in the database
	for inDB.Next() {
		var i string
		err := inDB.Scan(&i)
		if err != nil {
			queryError(err, q)
			return
		}
		for pos, e := range elements {
			if e == i+".jpg" {
				// remove from elements if exists
				elements[pos] = elements[len(elements)-1]
				elements = elements[:len(elements)-1]
				break
			}
		}
	}

	// remove all elements still left
	for _, e := range elements {
		if e == "" {
			continue
		}
		err := os.Remove(c.HanayoFolder + "/static/profbackgrounds/" + e)
		if err != nil {
			color.Red("> ClearExpiredProfileBackgrounds: failed to delete a background: %v", err)
		}
	}
}
