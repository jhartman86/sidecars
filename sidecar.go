/*
Implement FOR SELECT for row-level locking:
  - https://spin.atomicobject.com/2021/02/04/redis-postgresql/
  - https://www.postgresql.org/docs/9.5/sql-select.html#SQL-FOR-UPDATE-SHARE
  - https://dba.stackexchange.com/questions/170976/postgres-listen-notify-as-message-queue
  - https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
*/
package sidecars

import (
	"context"
	"fmt"
	"pkg/sys/db"
	"pkg/sys/errz"
	"pkg/sys/logging"
	"pkg/sys/timezones"
	"pkg/sys/typed"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

var (
	dur1Min = time.Duration(1 * time.Minute)

	manager = &struct {
		sync.Mutex
		sync.WaitGroup
		drainAll sync.Once
		cars     map[string]*Sidecar
	}{
		cars: make(map[string]*Sidecar),
	}
)

type (
	Sidecarish interface {
		Enque(j any, cnxn ...db.Store) (typed.UUID, error)
		TestingMockClock(_ *testing.T, start time.Time, increment time.Duration)
		TestingSetMaxTicks(_ *testing.T, ticks int)
		TestingSetDelayPerTick(_ *testing.T, d time.Duration)
		TestingSetOnPanic(_ *testing.T, fn func(logging.LoggerShape, any))
		TestingSetLogger(_ *testing.T, l logging.LoggerShape)
		TestingExec(t *testing.T)
	}

	Sidecar struct {
		isRunning bool // probably want a locking mechanism around this...
		syncs     struct {
			drain      sync.Once
			exec       sync.Once
			chQuit     chan struct{}
			chJobs     chan any
			chJobQ     chan any
			chTicker   *time.Ticker
			dispatcher sync.WaitGroup
			workers    sync.WaitGroup
		}
		config struct {
			logger   logging.LoggerShape
			clock    func(z timezones.ZoneID) time.Time // assignable for mocking
			onPanic  func(logging.LoggerShape, any)     // capturable; assignable for mocking
			lastExec time.Time
		}
		// test mode helpers
		testing struct {
			t        *testing.T
			mode     bool
			maxTicks int
			ticks    int
			// lastMockClockTime time.Time
			delayPerTick time.Duration
		}
		// Publicly definable
		RunType     runType
		OnError     func(logging.LoggerShape, error)
		OnBeforeRun func(logging.LoggerShape) // hook so jobs defined on distant intervals can log for some semblance of confidence its actually running
		Concurrency int
		Timeout     time.Duration
		Handle      string
		Handler     func(JobContext) error
		Schedule    *Schedule
	}

	SidecarTime struct{ H, M int }

	Schedule struct {
		use              bool          // will be set to true only if defined and valid by callers
		Cadence          time.Duration // default: 1 min
		OnDayOfMonth     int
		Timezone         timezones.ZoneID
		Weekdays         []time.Weekday
		useWeekdays      map[time.Weekday]struct{}
		SpecificTimes    []SidecarTime
		useSpecificTimes map[int] /*h*/ map[int] /*m*/ struct{}
		// if you add a new job record now, and the schedule runs every minute, but you set this
		// DeferNewJobsFor to 5 days, it won't run until now+5days
		DeferNewJobsFor time.Duration
	}
)

/*
 */
func NewSidecar(sc *Sidecar, tt ...*testing.T) Sidecarish {
	manager.Lock()
	defer manager.Unlock()
	sc.setup()
	// test things
	for i := range tt {
		sc.testing.t = tt[i]
		sc.testing.mode = true
		sc.Schedule.Cadence = time.Duration(1 * time.Nanosecond)
		break
	}
	manager.cars[sc.Handle] = sc
	return sc
}

/*
Enque sends a job to a sidecar for processing. Note that the returned UUID will
only ever be valid if the run type is Deferred, where its actually creating a job
record.
*/
func (sc *Sidecar) Enque(j any, cnxn ...db.Store) (typed.UUID, error) {
	if !sc.isRunning {
		return typed.UUID{}, fmt.Errorf(`Sidecar %s cannot enqueue jobs (not yet running)`, sc.Handle)
	}

	if sc.RunType.eq(RunTypeStandalone) {
		return typed.UUID{}, fmt.Errorf(`RunType:%s cannot accept jobs`, sc.RunType)
	}

	select {
	case <-sc.syncs.chQuit:
		err := fmt.Errorf(`Sidecar(%s).Enque: declined (draining in progress)`, sc.Handle)
		sc.OnError(sc.config.logger, err)
		return typed.UUID{}, errz.Trace(err)
	default:
		// if deferred...
		if sc.RunType.eq(RunTypeDeferred) {
			var du typed.Timestamp
			if sc.Schedule.DeferNewJobsFor > 0 {
				du = typed.NewTimestamp(time.Now().In(time.UTC).Add(sc.Schedule.DeferNewJobsFor))
				// @todo: this isn't _really_ right using lastExec here... come up with another way
				if sc.testing.mode {
					du = typed.NewTimestamp(sc.config.lastExec.Add(sc.Schedule.DeferNewJobsFor))
				}
			}
			rec := SidecarJob{
				Handle:     typed.NewString(sc.Handle),
				JobData:    toJobData(j),
				Status:     JobStatusReady,
				DeferUntil: du,
			}
			if err := db.Conn(cnxn...).Direct().Create(&rec).Error; err != nil {
				return typed.UUID{}, errz.Trace(err)
			}
			return rec.ID, nil
		}
		// in memory...
		sc.syncs.chJobQ <- j
		return typed.UUID{}, nil
	}
}

/*
 */
func (sc *Sidecar) drain() {
	sc.syncs.drain.Do(func() {
		sc.config.logger.Infof("Sidecar(%s) draining...", sc.Handle)
		sc.syncs.chQuit <- struct{}{}
		close(sc.syncs.chQuit)
		sc.syncs.dispatcher.Wait()
		close(sc.syncs.chJobQ)
		close(sc.syncs.chJobs)
	})
	sc.syncs.workers.Wait()
	sc.config.logger.Infof("Sidecar(%s) drained OK", sc.Handle)
}

/*
 */
func (sc *Sidecar) exec() {
	if sc.testing.mode && sc.testing.maxTicks == 0 {
		panic(errz.Errorf(`TestMode/panic: refusing sidecar.exec(%s): testing.maxTicks cannot be 0; use TestingSetMaxTicks`, sc.Handle))
	}

	defer func() {
		sc.isRunning = true
	}()

	// Always based on a ticker, even if its immediate/in-memory;
	// ticker acts as dispatcher, tracks itself for testing, and (importantly)
	// **is single-threaded**. Can be consumers galore, but only ONE dispatcher.
	sc.syncs.exec.Do(func() {
		sc.syncs.dispatcher.Add(1)
		go func(sc *Sidecar) {
			defer sc.syncs.dispatcher.Done()
			// define ticker
			sc.syncs.chTicker = time.NewTicker(sc.Schedule.Cadence)
			// launch consumers
			for c := 1; c <= sc.Concurrency; c++ {
				sc.spawnWorker()
			}
			// dispatcher
		periodical:
			for {
				select {
				case <-sc.syncs.chQuit:
					sc.syncs.chTicker.Stop()
					for { // drain remaining loop
						select {
						case j, ok := <-sc.syncs.chJobQ:
							if !ok {
								return
							}
							sc.syncs.chJobs <- j
						default:
							return
						}
					}
				case <-sc.syncs.chTicker.C:
					// test helpers only
					if sc.testing.mode && sc.testing.maxTicks >= 1 {
						if sc.testing.ticks >= sc.testing.maxTicks {
							sc.syncs.chTicker.Stop()
							go sc.drain() // must be a routine otherwise this would be self-referentially blocking :|
							continue periodical
						}
						sc.testing.ticks++
						time.Sleep(sc.testing.delayPerTick)
					}

					// hit the shouldRun check
					if now, should := sc.shouldRun(); should {
						sc.config.lastExec = now

						// this could potentially block, but we're all adults so just don't
						sc.OnBeforeRun(sc.config.logger)

						switch sc.RunType {
						case RunTypeStandalone:
							sc.syncs.chJobs <- sc.config.lastExec

						case RunTypeDeferred:
							var recurse func() error
							recurse = func() error {
								jobs, err := loadReadySidecarJobs(sc.Handle, now)
								if err != nil {
									return errz.Trace(err)
								}
								for i := range jobs {
									sc.syncs.chJobs <- jobs[i]
								}
								if len(jobs) < batchLoadSize {
									return nil
								}
								return errz.Trace(recurse())
							}
							if err := recurse(); err != nil {
								sc.OnError(sc.config.logger, errz.Trace(err))
								continue periodical
							}
							// jobs, err := loadReadySidecarJobs(sc.Handle, now)
							// if err != nil {
							// 	sc.OnError(sc.config.logger, errz.Trace(err))
							// 	continue periodical
							// }
							// for i := range jobs {
							// 	sc.syncs.chJobs <- jobs[i]
							// }

						case RunTypeMemory:
							for {
								select {
								case j, ok := <-sc.syncs.chJobQ:
									if !ok {
										continue periodical // return?
									}
									sc.syncs.chJobs <- j
								default:
									continue periodical
								}
							}
						}
					}
				}
			}
		}(sc)
	})
}

/*
covers panic handling, and deferred .Done()s
*/
func (sc *Sidecar) afterWorker() {
	// if necessary to recover, do so
	if r := recover(); r != nil {
		sc.config.onPanic(sc.config.logger, r)
		sc.spawnWorker()
	}
	// ALWAYS invoke these
	manager.Done()
	sc.syncs.workers.Done()
}

/*
 */
func (sc *Sidecar) spawnWorker() {
	manager.Add(1)
	sc.syncs.workers.Add(1)
	go func() {
		defer sc.afterWorker()
		for j := range sc.syncs.chJobs {
			if err := sc.process(j); err != nil {
				sc.OnError(sc.config.logger, errz.Trace(err))
			}
		}
	}()
}

/*
 */
func (sc *Sidecar) process(j any) error {
	ctxRoot, cancel := context.WithTimeout(context.Background(), sc.Timeout)
	defer cancel()
	ctxRoot = context.WithValue(ctxRoot, ctxValueKey, j)
	jobCTX := &jobContext{
		Context: ctxRoot,
		logger:  sc.config.logger,
		sidecar: sc,
	}

	res := make(chan error, 1)
	// @todo: use waitgroups to track that this function doesn't go totally
	// rogue (but also... *should* it block???)
	go func() {
		// handler is invoked below in this routine, and THAT is what we're
		// setting up this recover for
		defer func() {
			if r := recover(); r != nil {
				sc.config.onPanic(sc.config.logger, r)
				if err, ok := r.(error); ok {
					res <- errz.Trace(err)
					return
				}
				res <- errz.Errorf(`CaughtUntypedPanic: %v`, r)
			}
		}()
		// defer panic capture handles this; the select statement below should ALWAYS
		// receive a result
		res <- sc.Handler(jobCTX)
	}()

	select {
	case err := <-res:
		if sc.RunType.eq(RunTypeDeferred) {
			jv, ok := j.(SidecarJob)
			if !ok {
				// @todo
				return err
			}
			switch err != nil {
			// there was an error; record a failure
			case true:
				if dbErr := jv.recordStatus(JobStatusError, errToStackText(err)); dbErr != nil {
					return errz.Wrap(err, errz.Trace(dbErr))
				}
				return errz.Trace(err)
			// no error; record success
			case false:
				if dbErr := jv.recordStatus(JobStatusDone); dbErr != nil {
					return errz.Wrap(err, errz.Trace(dbErr))
				}
				return nil
			}
		}
		return errz.Trace(err)

	case <-jobCTX.Done():
		// @todo: unless the test time.sleeps, for some reason this doesn't
		// get recorded (waitgroups finalize before the DB records...)
		err := errz.Errorf(`TimedOut: %s`, jobCTX.Err())
		if sc.RunType.eq(RunTypeDeferred) {
			jv, ok := j.(SidecarJob)
			if !ok {
				// @todo
				return errz.Errorf(`bad conversion`)
			}
			if dbErr := jv.recordStatus(JobStatusTimedOut, errToStackText(err)); dbErr != nil {
				return errz.Wrap(err, errz.Trace(dbErr))
			}
		}
		return errz.Trace(err)
	}
}

/*
 */
func (sc *Sidecar) shouldRun() (time.Time, bool) {
	now := sc.config.clock(sc.Schedule.Timezone)

	if !sc.Schedule.use {
		return now, true
	}

	usesSpecificTime, hitSpecificTime := len(sc.Schedule.useSpecificTimes) > 0, false
	if usesSpecificTime {
		if h, hitH := sc.Schedule.useSpecificTimes[now.Hour()]; hitH {
			_, hitSpecificTime = h[now.Minute()]
		}
	}

	// always expect weekdays to be configured
	if _, ok := sc.Schedule.useWeekdays[now.Weekday()]; !ok {
		return now, false
	}

	// if day of month was specified
	if sc.Schedule.OnDayOfMonth > 0 {
		if now.Day() != sc.Schedule.OnDayOfMonth {
			return now, false
		}
		// @todo: hole here: this assumes a constant, uninterrupted running of the binary
		// (ie. never being forced to restart on the same day); this could occur pretty easily
		if !sc.config.lastExec.IsZero() && !usesSpecificTime && (now.Month() == sc.config.lastExec.Month()) {
			return now, false
		}
	}

	// if specific times are used...
	if usesSpecificTime {
		if !hitSpecificTime {
			return now, false
		}
		if !sc.config.lastExec.IsZero() && now.Day() == sc.config.lastExec.Day() && now.Minute() == sc.config.lastExec.Minute() {
			return now, false
		}
	}

	return now, true
}

/*
_defaultClock is the default clock function assigned to config.clock
*/
func (sc *Sidecar) _defaultClock(z timezones.ZoneID) time.Time {
	return time.Now().In(z.Location())
}

/*
_onPanic is the default handler assigned to config.onPanic
*/
func (sc *Sidecar) _onPanic(l logging.LoggerShape, r any) {
	switch typed := r.(type) {
	case error:
		l.Errorw(`sidecar._onPanic handler`, // always log info about panic
			zap.String(`sidecar`, sc.Handle),
			zap.String(`error`, errz.ErrorStack(typed).String()),
		)
		return
	default:
		l.Errorw(`sidecar._onPanic handler`, // always log info about panic
			zap.String(`sidecar`, sc.Handle),
			zap.Any(`untyped_recover`, r),
		)
	}
}
