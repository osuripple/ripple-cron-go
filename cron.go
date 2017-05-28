package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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
	PopulateRedis                  bool
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

var db *sqlx.DB
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

	verboseln("Starting MySQL connection")
	// start database connection
	db, err = sqlx.Open("mysql", c.DSN)
	if err != nil {
		color.Red("couldn't start MySQL connection: %v", err)
		return
	}
	defer db.Close()

	r = redis.NewClient(&redis.Options{
		Addr:     c.RedisAddr,
		Password: c.RedisPassword,
	})

	// spawn some workers
	verboseln("Spawning necessary workers")
	for i := 0; i < c.Workers; i++ {
		chanWg.Add(1)
		go worker(execOperations)
	}
	chanWg.Add(1)
	go worker(syncOperations)

	timeAtStart := time.Now()

	if c.CalculateAccuracy {
		verboseln("Starting accuracy calculator worker")
		wg.Add(1)
		go opCalculateAccuracy()
	}
	if c.DeleteOldPasswordResets {
		verboseln("Starting deleting old password resets")
		go opSync("DELETE FROM password_recovery WHERE t < (NOW() - INTERVAL 1 DAY);")
	}
	if c.FixCompletedScores {
		verboseln("Starting fixing completed = 3 scores on not ranked beatmaps")
		go opSync(`UPDATE scores
			INNER JOIN beatmaps ON beatmaps.beatmap_md5 = scores.beatmap_md5
			SET completed = '2'
			WHERE beatmaps.ranked < 1 OR beatmaps.ranked > 5;`)
	}
	if c.DeleteOldPrivateTokens {
		verboseln("Deleting old private API tokens")
		go opSync(`DELETE FROM tokens WHERE private = 1 AND last_updated < ?`, time.Now().Add(-time.Hour*24*30))
	}
	if c.UnrankScoresOnInvalidBeatmaps {
		verboseln("Unranking scores on invalid beatmaps")
		go opSync(`DELETE scores.* FROM scores
		LEFT JOIN beatmaps ON scores.beatmap_md5 = beatmaps.beatmap_md5
		WHERE beatmaps.beatmap_md5 IS NULL`)
	}
	if c.RemoveDonorOnExpired {
		verboseln("Removing donor privileges on users where donor expired")
		go func() {
			_, err := http.Post("http://127.0.0.1:3366/api/v1/clear_donor", "", nil)
			if err != nil {
				color.Red("%v", err)
			}
		}()
	}
	if c.CacheLevel || c.CacheTotalHits || c.CacheRankedScore {
		verboseln("Starting caching of various user stats")
		wg.Add(1)
		go opCacheData()
	}
	if c.CleanReplays {
		verboseln("Starting cleaning useless replays")
		wg.Add(1)
		go opCleanReplays()
	}
	if c.DeleteReplayCache {
		verboseln("Starting deleting replay cache")
		wg.Add(1)
		go opDeleteReplayCache()
	}
	if c.CalculatePP {
		verboseln("Starting calculating pp")
		wg.Add(2)
		go opCalculatePP()
	}
	if c.FixScoreDuplicates {
		verboseln("Starting fixing score duplicates")
		wg.Add(1)
		go opFixScoreDuplicates()
	}
	if c.CalculateOverallAccuracy {
		verboseln("Starting calculating overall accuracy")
		wg.Add(1)
		go opCalculateOverallAccuracy()
	}
	if c.FixMultipleCompletedScores {
		verboseln("Starting fixing multiple completed scores")
		wg.Add(1)
		go opFixMultipleCompletedScores()
	}
	if c.ClearExpiredProfileBackgrounds {
		verboseln("Removing profile backgrounds of expired donors")
		wg.Add(1)
		go opClearExpiredProfileBackgrounds()
	}
	if c.SetOnlineUsers {
		wg.Add(1)
		go opSetOnlineUsers()
	}

	wg.Wait()
	color.Green("Data elaboration has finished.")
	color.Green("Execution time: %.4fs", time.Now().Sub(timeAtStart).Seconds())
	color.Yellow("Waiting for workers to finish...")
	close(execOperations)
	close(syncOperations)
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
func opSync(query string, params ...interface{}) {
	syncOperations <- operation{query, params}
}

// Operations that can be executed with a simple db.Exec, distributed across 8 workers.
var execOperations = make(chan operation, 100000)

// Operations that must be executed in order and synchronously.
var syncOperations = make(chan operation, 20)

func worker(c <-chan operation) {
	for op := range c {
		runOperation(op)
	}
	chanWg.Done()
}

func runOperation(op operation) {
	logquery(op.query, op.params)
	_, err := db.Exec(op.query, op.params...)
	if err != nil {
		queryError(err, op.query, op.params...)
	}
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
