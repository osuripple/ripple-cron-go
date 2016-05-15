package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/thehowl/conf"
)

type config struct {
	DSN       string
	RippleDir string `description:"The ripple folder (e.g. /var/www/ripple, NOT /var/www/ripple/osu.ppy.sh). Write the directory relatively to where the ripple-cron-go executable is placed."`

	CalculateAccuracy bool
	CacheRankedScore  bool
	CacheTotalHits    bool
	CacheLevel        bool

	DeleteOldPasswordResets bool
	CleanReplays            bool
	DeleteReplayCache       bool
	BuildLeaderboards       bool
	CalculatePP             bool
	FixScoreDuplicates      bool `description:"might take a VERY long time"`

	LogQueries bool `description:"You don't wanna do this in prod."`
	Workers    int  `description:"The number of goroutines which should execute queries. Increasing it may make cron faster, depending on your system."`
}

var db *sql.DB
var c = config{
	DSN:     "root@/ripple",
	Workers: 8,
}
var wg sync.WaitGroup
var chanWg sync.WaitGroup

func main() {
	// Set up the configuration.
	err := conf.Load(&c, "cron.conf")
	switch {
	case err == conf.ErrNoFile:
		color.Yellow("No cron.conf was found. Creating it...")
		err := conf.Export(&c, "cron.conf")
		if err != nil {
			color.Red("Couldn't create cron.conf: %v.", err)
		} else {
			color.Green("cron.conf has been created!")
		}
		return
	case err != nil:
		color.Red("cron.conf couldn't be loaded: %v.", err)
		return
	}

	fmt.Println(`
           ___ _ __ ___  _ __  
          / __| '__/ _ \| '_ \ 
         | (__| | | (_) | | | |
          \___|_|  \___/|_| |_|
`)
	color.Green("     (not so) proudly brought to you by")
	color.Green("              The Ripple Team™")
	fmt.Println()

	fmt.Print("Starting MySQL connection...")
	// start database connection
	db, err = sql.Open("mysql", c.DSN)
	if err != nil {
		color.Red(" couldn't start MySQL connection: %v", err)
		return
	}
	color.Green(" ok!")
	defer db.Close()

	// spawn some workers
	fmt.Print("Spawning necessary workers...")
	for i := 0; i < c.Workers; i++ {
		chanWg.Add(1)
		go worker()
	}
	color.Green(" ok!")

	timeAtStart := time.Now()

	if c.CalculateAccuracy {
		fmt.Print("Starting accuracy calculator worker...")
		wg.Add(1)
		go opCalculateAccuracy()
		color.Green(" ok!")
	}
	if c.DeleteOldPasswordResets {
		fmt.Print("Starting deleting old password resets...")
		go op("DELETE FROM password_recovery WHERE t < (NOW() - INTERVAL 10 DAY);")
		color.Green(" ok!")
	}
	if c.CacheLevel || c.CacheTotalHits || c.CacheRankedScore {
		fmt.Print("Starting caching of various user stats...")
		wg.Add(1)
		go opCacheData()
		color.Green(" ok!")
	}
	if c.CleanReplays {
		fmt.Print("Starting cleaning useless replays...")
		wg.Add(1)
		go opCleanReplays()
		color.Green(" ok!")
	}
	if c.DeleteReplayCache {
		fmt.Print("Starting deleting replay cache...")
		wg.Add(1)
		go opDeleteReplayCache()
		color.Green(" ok!")
	}
	if c.CalculatePP {
		fmt.Print("Starting calculating pp...")
		wg.Add(2)
		go opCalculatePP()
		color.Green(" ok!")
	}
	if c.FixScoreDuplicates {
		fmt.Print("Starting fixing score duplicates...")
		wg.Add(1)
		go opFixScoreDuplicates()
		color.Green(" ok!")
	}

	wg.Wait()
	color.Green("Data elaboration has been terminated.")
	color.Green("Execution time: %.4fs", time.Now().Sub(timeAtStart).Seconds())
	color.Yellow("Waiting for workers to finish...")
	close(execOperations)
	chanWg.Wait()
}

// db operation to be made, generally used for execOperations
type operation struct {
	query  string
	params []interface{}
}

func op(query string, params ...interface{}) {
	execOperations <- operation{query, params}
}

// Operations that can be executed with a simple db.Exec, distributed across 8 workers.
var execOperations = make(chan operation, 100000)

func worker() {
	for op := range execOperations {
		if c.LogQueries {
			// porcodio go se sei odioso a volte
			a := []interface{}{
				"=>",
				op.query,
				"| params:",
			}
			a = append(a, op.params...)
			fmt.Println(a...)
		}
		_, err := db.Exec(op.query, op.params...)
		if err != nil {
			queryError(err, op.query, op.params...)
		}
	}
	chanWg.Done()
}

func queryError(err error, query string, params ...interface{}) {
	color.Red(`==> Query error!
===> %s
===> params: %v
===> error: %v`, query, params, err)
}
