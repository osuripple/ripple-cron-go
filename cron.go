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
	ReplayFolder  string
	HanayoFolder  string
	RedisAddr     string
	RedisPassword string

	CalculateAccuracy       bool
	CacheRankedScore        bool
	CacheTotalHits          bool
	CacheLevel              bool
	CachePlayTime           bool
	CacheMostPlayedBeatmaps bool

	DeleteOldPasswordResets        bool
	CleanReplays                   bool
	PopulateRedis                  bool
	CalculatePP                    bool
	FixScoreDuplicates             bool `description:"might take a VERY long time"`
	CalculateOverallAccuracy       bool
	FixCompletedScores             bool `description:"Set to completed = 2 all scores on beatmaps that aren't ranked."`
	UnrankScoresOnInvalidBeatmaps  bool `description:"Set to completed = 2 all scores on beatmaps that are not in the database."`
	RemoveDonorOnExpired           bool
	DonorbotBaseApiUrl             string
	FixMultipleCompletedScores     bool `description:"Set completed=2 if multiple completed=3 scores for same beatmap and user are present."`
	ClearExpiredProfileBackgrounds bool
	DeleteOldPrivateTokens         bool `description:"Whether to delete old private (private = 1) API tokens (older than a month)"`
	SetOnlineUsers                 bool
	PrunePendingVerificationAfter  int  `description:"Number of days after which a user will be removed if they are still pending verification."`
	CalculateServerWiseStats       bool `description:"Re-calculates some server-wise cached stats (mostly displayed in RAP)"`
	FixStatsOverflow               bool `description:"Re-calculates ranked & total score for users whose values have overflowed. Faster than CacheData if there's an overflow issue. This will be ignored if CacheData=true."`

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
var configFile string

func init() {
	flag.BoolVar(&v, "v", false, "verbose")
	flag.BoolVar(&vv, "vv", false, "very verbose (LogQueries)")
	configFlag := flag.String("config", "cron.conf", "Configuration file")
	flag.Parse()
	configFile = string(*configFlag)

	v = vv || v
}

func main() {
	// Set up the configuration.
	flag.Parse()

	err := conf.Load(&c, configFile)
	switch {
	case err == conf.ErrNoFile:
		color.Yellow("No %s was found. Creating it", configFile)
		err := conf.Export(&c, configFile)
		if err != nil {
			color.Red("Couldn't create %s: %v.", configFile, err)
		} else {
			color.Green("%s has been created!", configFile)
		}
		return
	case err != nil:
		color.Red("%s couldn't be loaded: %v.", configFile, err)
		return
	}

	verboseln("Starting MySQL connection")
	// start database connection
	db, err = sqlx.Open("mysql", c.DSN)
	if err != nil {
		color.Red("couldn't start MySQL connection: %v.", err)
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
	if c.PrunePendingVerificationAfter > 0 {
		verboseln("Pruning users pending verification...")
		go opSync(`DELETE users, users_stats FROM users
		INNER JOIN users_stats
		WHERE users.id = users_stats.id AND users.latest_activity = 0
		AND users.privileges = 1048576 AND users.register_datetime < ?`,
			time.Now().Add(-time.Hour*24*time.Duration(c.PrunePendingVerificationAfter)).Unix())
	}
	if c.RemoveDonorOnExpired {
		verboseln("Removing donor privileges on users where donor expired")
		go func() {
			_, err := http.Post(c.DonorbotBaseApiUrl+"/api/v1/clear_donor", "", nil)
			if err != nil {
				color.Red("%v", err)
			}
		}()
	}
	cacheData := c.CacheLevel || c.CacheTotalHits || c.CacheRankedScore || c.CachePlayTime || c.CacheMostPlayedBeatmaps
	if cacheData {
		verboseln("Starting caching of various user stats")
		wg.Add(1)
		go opCacheData()
	}
	if cacheData && c.FixStatsOverflow {
		color.Yellow("> Ignoring FixStatsOverflow because CacheData is already enabled")
	} else if c.FixStatsOverflow {
		verboseln("Starting fixing total scores and ranked scores overflow")
		wg.Add(1)
		go opFixStatsOverflow()
	}
	if c.CleanReplays {
		verboseln("Starting cleaning useless replays")
		wg.Add(1)
		go opCleanReplays()
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
	if c.CalculateServerWiseStats {
		verboseln("Starting calculating server-wise stats")
		wg.Add(1)
		go opServerwiseStats()
	}

	wg.Wait()
	color.Green("Data elaboration has finished")
	color.Green("Execution time: %.4fs", time.Now().Sub(timeAtStart).Seconds())
	color.Yellow("Waiting for workers to finish")
	close(execOperations)
	close(syncOperations)
	chanWg.Wait()
	conf.Export(c, configFile)
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
