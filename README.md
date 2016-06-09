# ripple-cron-go

The fastest cron for ripple you'll never need.

A bit of explaination here:

Ripple uses a cronjob to fix any eventual errors in the database. For doing it, it used [cron.php](https://github.com/osuripple/ripple/blob/master/osu.ppy.sh/cron.php), a terribly performing script. [No, really.](https://y.zxq.co/minzed.jpg) That's three minutes of server CPU being used at 100%!

So I decided to rewrite it to be better performant, and what better language to do it if not Go (well, C, C++ and Assembly are indeed faster, but I'm not on that level of insanity).

This is the result: https://asciinema.org/a/42583 (watch the video especially for the last 30 seconds, as you can see the true power of ripple-cron-go).

## Installing

Assuming you have Go installed and your GOPATH set up

```sh
go get -u github.com/osuripple/ripple-cron-go
cd $GOPATH/src/github.com/osuripple/ripple-cron-go
go build
./ripple-cron-go
nano cron.conf
./ripple-cron-go # Boom!
```

## Extending

This is an example of a very simple unit of ripple-cron-go:

```go
package main

import (
	"time"

	"github.com/fatih/color"
)

func opTimeConsumingTask() {
	defer wg.Done()
	
	time.Sleep(time.Second)
	color.Green("> TimeConsumingTask: done!", count)
}
```

Then you would add a bool in the `config` struct to enable/disable the task, then this to cron.go (cron.go contains `main()`)

```go
	if c.TimeConsumingTask {
		fmt.Print("Starting time consuming task...")
		wg.Add(1)
		go opTimeConsumingTask()
		color.Green(" ok!")
	}
```

## License

MIT
