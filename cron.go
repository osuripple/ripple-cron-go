package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/thehowl/conf"
	"gopkg.in/redis.v5"
)

type config struct {
	DSN           string
	RippleDir     string `description:"The ripple folder (e.g. /var/www/ripple, NOT /var/www/ripple/osu.ppy.sh). Write the directory relatively to where the ripple-cron-go executable is placed."`
	HanayoFolder  string
	RedisAddr     string
	RedisPassword string

	CalculateAccuracy bool
	CacheRankedScore  bool
	CacheTotalHits    bool
	CacheLevel        bool

	DeleteOldPasswordResets        bool
	CleanReplays                   bool
	DeleteReplayCache              bool
	BuildLeaderboards              bool
	CalculatePP                    bool
	FixScoreDuplicates             bool `description:"might take a VERY long time"`
	CalculateOverallAccuracy       bool
	FixCompletedScores             bool `description:"Set to completed = 2 all scores on beatmaps that aren't ranked."`
	UnrankScoresOnInvalidBeatmaps  bool `description:"Set to completed = 2 all scores on beatmaps that are not in the database."`
	RemoveDonorOnExpired           bool
	FixMultipleCompletedScores     bool `description:"Set completed=2 if multiple completed=3 scores for same beatmap and user are present."`
	ClearExpiredProfileBackgrounds bool
	DeleteOldPrivateTokens         bool `description:"Whether to delete old private (private = 1) API tokens (older than a month)"`
	SetOnlineUsers                 bool

	Workers int `description:"The number of goroutines which should execute queries. Increasing it may make cron faster, depending on your system."`
}

var db *sql.DB
var c = config{
	DSN:     "root@/ripple",
	Workers: 8,
}
var r *redis.Client
var wg sync.WaitGroup
var chanWg sync.WaitGroup
var v bool
var vv bool

func init() {
	flag.BoolVar(&v, "v", false, "verbose")
	flag.BoolVar(&vv, "vv", false, "very verbose (LogQueries)")
	flag.Parse()

	v = vv || v
}

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

	r = redis.NewClient(&redis.Options{
		Addr:     c.RedisAddr,
		Password: c.RedisPassword,
	})

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
		go op("DELETE FROM password_recovery WHERE t < (NOW() - INTERVAL 1 DAY);")
		color.Green(" ok!")
	}
	if c.FixCompletedScores {
		fmt.Print("Starting fixing completed = 3 scores on not ranked beatmaps...")
		go op(`UPDATE scores
			INNER JOIN beatmaps ON beatmaps.beatmap_md5 = scores.beatmap_md5
			SET completed = '2'
			WHERE
				beatmaps.ranked != '1' AND
				beatmaps.ranked != '2' AND
				beatmaps.ranked != '3' AND
				beatmaps.ranked != '4';`)
		color.Green(" ok!")
	}
	if c.DeleteOldPrivateTokens {
		fmt.Println("Starting deleting old private API tokens")
		go op(`DELETE FROM tokens WHERE private = 1 AND last_updated < ?`, time.Now().Add(-time.Hour*24*30))
		color.Green(" ok!")
	}
	if c.UnrankScoresOnInvalidBeatmaps {
		fmt.Print("Unranking scores on invalid beatmaps...")
		go op(`DELETE scores.* FROM scores
		LEFT JOIN beatmaps ON scores.beatmap_md5 = beatmaps.beatmap_md5
		WHERE beatmaps.beatmap_md5 IS NULL`)
		color.Green(" ok!")
	}
	if c.RemoveDonorOnExpired {
		fmt.Print("Removing donor privileges on users where donor expired...")
		go func() {
			_, err := http.Post("http://127.0.0.1:3366/api/v1/clear_donor", "", nil)
			if err != nil {
				color.Red("%v", err)
			}
		}()
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
	if c.CalculateOverallAccuracy {
		fmt.Print("Starting calculating overall accuracy...")
		wg.Add(1)
		go opCalculateOverallAccuracy()
		color.Green(" ok!")
	}
	if c.FixMultipleCompletedScores {
		fmt.Print("Starting fixing multiple completed scores...")
		wg.Add(1)
		go opFixMultipleCompletedScores()
		color.Green(" ok!")
	}
	if c.ClearExpiredProfileBackgrounds {
		fmt.Print("Removing profile backgrounds of expired donors...")
		wg.Add(1)
		go opClearExpiredProfileBackgrounds()
		color.Green(" ok!")
	}
	if c.SetOnlineUsers {
		wg.Add(1)
		go opSetOnlineUsers()
	}

	wg.Wait()
	color.Green("Data elaboration has been terminated.")
	color.Green("Execution time: %.4fs", time.Now().Sub(timeAtStart).Seconds())
	color.Yellow("Waiting for workers to finish...")
	close(execOperations)
	chanWg.Wait()
	conf.Export(c, "cron.conf")
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
		logquery(op.query, op.params)
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

func logquery(q string, params []interface{}) {
	if vv {
		// porcodio go se sei odioso
		a := []interface{}{
			"=>",
			q,
			"| params:",
		}
		a = append(a, params...)
		fmt.Println(a...)
	}
}

func verbosef(format string, args ...interface{}) {
	if v {
		fmt.Printf(format, args...)
	}
}
func verboseln(args ...interface{}) {
	if v {
		fmt.Println(args...)
	}
}
