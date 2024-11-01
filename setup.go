package sidecars

import (
	"fmt"
	"pkg/sys"
	"pkg/sys/errz"
	"pkg/sys/logging"
	"pkg/sys/timezones"
	"time"

	"go.uber.org/zap"
)

func (sc *Sidecar) setup() {
	if len(sc.Handle) == 0 {
		panic(errz.Errorf(`[PurposePanic] Cannot register sidecar without 'Handle'`))
	}
	sc.Handle = sys.StrToSnakeCase(sc.Handle)
	if _, already := manager.cars[sc.Handle]; already {
		panic(errz.Errorf(`[PurposePanic] Cannot re-register sidecar with same handle: %s`, sc.Handle))
	}
	if sc.Handler == nil {
		panic(errz.Errorf(`[PurposePanic] Sidecar(%s) 'Handler' parameter required`, sc.Handle))
	}
	if !sc.RunType.valid() {
		panic(errz.Errorf(`[PurposePanic] Sidecar(%s) RunType not valid, have: '%s'`, sc.Handle, sc.RunType))
	}
	sc.syncs.chQuit = make(chan struct{}, 1)
	sc.syncs.chJobs = make(chan any)
	sc.syncs.chJobQ = make(chan any)
	sc.config.logger = logging.GetLogger().Named(fmt.Sprintf(`sidecar.%s`, sc.Handle))
	sc.config.clock = sc._defaultClock
	sc.config.onPanic = sc._onPanic
	if sc.Concurrency == 0 {
		sc.Concurrency = 1
	}
	if sc.Timeout == 0 {
		sc.Timeout = time.Duration(5 * time.Minute)
	}
	if sc.OnError == nil { // default to simple log out
		sc.OnError = func(l logging.LoggerShape, err error) {
			l.Errorw(sc.Handle,
				zap.String(`error`, errz.ErrorStack(err).String()),
			)
		}
	}
	if sc.OnBeforeRun == nil { // default to noop
		sc.OnBeforeRun = func(_ logging.LoggerShape) { return }
	}
	// schedule settings (ensuring Schedule is at least always defined)
	switch sc.Schedule == nil {
	case true:
		// Validate that RunType is in-memory; if its anything else, it doesn't make sense to use
		// with a schedule
		if !sc.RunType.eq(RunTypeMemory) {
			panic(errz.Errorf(`[PurposePanic] Sidecar(%s) has no schedule and RunType *other than* memory, have: '%s'`, sc.Handle, sc.RunType))
		}
		// in memory jobs will let a queue build up for up to 50ms
		sc.Schedule = &Schedule{use: false, Cadence: time.Duration(50 * time.Millisecond), Timezone: timezones.Z_UTC}
	case false:
		if sc.RunType.eq(RunTypeMemory) {
			panic(errz.Errorf(`[PurposePanic] Sidecar(%s) has a *Schedule{} param and RunType '%s', which are incompatible ('memory' sidecars do not honor a schedule)`, sc.Handle, sc.RunType))
		}
		sc.Schedule.use = true
		if !sc.Schedule.Timezone.Valid() {
			sc.Schedule.Timezone = timezones.Z_UTC
		}
		// weekday things
		if len(sc.Schedule.Weekdays) >= 1 && sc.Schedule.OnDayOfMonth > 0 {
			panic(errz.Errorf(`[PurposePanic] Sidecar(%s) schedule has both 'OnDayOfMonth' and 'Weekdays' configured, which is invalid. While using 'OnDayOfMonth', Weekdays cannot be specified. Please remove.`, sc.Handle))
		}
		sc.Schedule.useWeekdays = map[time.Weekday]struct{}{}
		if len(sc.Schedule.Weekdays) == 0 {
			sc.Schedule.Weekdays = []time.Weekday{
				time.Sunday, time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday, time.Saturday,
			}
		}
		for i := range sc.Schedule.Weekdays {
			sc.Schedule.useWeekdays[sc.Schedule.Weekdays[i]] = struct{}{}
		}
		// specific time things
		sc.Schedule.useSpecificTimes = map[int]map[int]struct{}{}
		if len(sc.Schedule.SpecificTimes) > 0 {
			for i := range sc.Schedule.SpecificTimes {
				st := sc.Schedule.SpecificTimes[i]
				if _, ok := sc.Schedule.useSpecificTimes[st.H]; !ok {
					sc.Schedule.useSpecificTimes[st.H] = map[int]struct{}{}
				}
				sc.Schedule.useSpecificTimes[st.H][st.M] = struct{}{}
			}
		}
		// Cadence always default to 1 min, if unset
		if sc.Schedule.Cadence == 0 {
			sc.Schedule.Cadence = dur1Min
		}
		// If specific time(s) or OnDayOfMonth is set, cadence automatically forced to every minute
		if sc.Schedule.OnDayOfMonth > 0 || len(sc.Schedule.useSpecificTimes) > 0 {
			sc.Schedule.Cadence = dur1Min
		}
	}
}
