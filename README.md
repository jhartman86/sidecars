# Golang Sidecars

This is not in shape to be used as an independent library; this was extracted from another project where this code has dependencies. (It's here for reference only).

### Intention

Running a Go webserver is, generally speaking, efficient af. The idea with sidecars is to eat up some opportunity cost. IOW - if the API services you run only consume ~10% CPU and ~20% memory, why not use your server to do more stuff (like processing jobs), instead of (probably) having another service be your job queue.

To have this function as an independent job/scheduled-task runner, it wouldn't be that heavy a lift.

## Of Note...

See `package_test.go` for how jobs are declared; there is a purposefully tight coupling where the task declares its schedule.

Supported:
* graceful shutdowns (as tasks can be scheduled "in memory only"; not durably persisted, from the scheduling perspective).

* timeouts (job overflows X time, spin it down)

* different scheduling plans:
	* in-memory (immediately start the work); not durably persisted
	* deferred (picked up on the next schedule tick); durably persisted
	* cadence-based (every N secs/min/etc)
		* cadence-based *cannot be sent jobs*; it self-discovers its own work; or just does "something", like send your mom an "I love you" text every 5 mins

* CRON++
	* Anything you could schedule with CRON syntax can be done, but also includes support for timezones and weekday specifications... and hopefully a much better declarative API.

# Usage

The api looks like this:

```
package a

// in memory: handle immediately
var SendMail = sidecars.NewSidecar(&sidecars.Sidecar{
	RunType: sidecars.RunTypeMemory, // or RunTypeDeferred, or RunTypeStandalone (see notes on Standalone in `run_type.go`)
	Handle: `mailjob_send_password_reset`, // globally unique, consistent string
	Concurrency: 3, // if SendMail gets a bunch of things to do at once, how many goroutines can be spun up to chew through the work
	Handler: func(j sidecars.JobContext) error {
		return nil
	},
})

// scheduled: do the task on the first day of every month, at 3am MTN
var SendMarketingSnailMail = sidecars.NewSidecar(&sidecars.Sidecar{
	RunType: sidecars.RunTypeDeferred,
	Handle: `sears_pamphlet`,
	Concurrency: 1,
	Schedule: &sidecars.Schedule{
		OnDayOfMonth: 1,
		Timezone: sidecars.TZ_Denver,
		SpecificTimes: []sidecars.sidecarTime{
			{H:3, M:0},
		}
	},
	Handler: func(j sidecars.JobContext) error {
		return nil
	},
})

// do something regularly (self-contained)
func init() {
	sidecars.NewSidecar(&sidecars.Sidecar{
		RunType: sidecars.RunTypeStandalone,
		Handle: `send_i_love_you_sms_to_mom`,
		Schedule: &sidecars.Schedule{
			Cadence: time.Duration(5 * time.Min)
		},
		Handler: func(j sidecars.JobContext) error {
			return nil
		},
	})
}
```

```
package b

// ... somewhere in some code that wants to send mail ...
a.SendMail(<whatever the payload is>) // payload must marshal/unmarshal JSON

// ... somewhere that decides next month, you should get a sears pamphlet ...
a.SendMarketSnailMail(<payload>)
```