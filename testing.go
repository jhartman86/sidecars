package sidecars

import (
	"pkg/sys/errz"
	"pkg/sys/logging"
	"pkg/sys/timezones"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestingReset(_ *testing.T) {
	DrainAll()
	manager.Lock()
	defer manager.Unlock()
	for h := range manager.cars {
		delete(manager.cars, h)
	}
	manager.drainAll = sync.Once{}
}

/*
TestingMockClock is used to mock fast-forwarding time under test. When used, it automatically
forces the ticker cadence to execute every one nanosecond, but **lies about the returned time**
based on the startTime and increment. Every time this function is invoked, it'll auto-add one
increment.

For example:

	TestingMockClock(t, sc, toTime(`2021-01-01T00:00:00Z`), time.Duration(1*time.Minute))
	will cause every call to the clock() function to return the time incremented by a minute

note: this function isn't concurrency-safe, BUT - the clock() function it mocks out should
only ever be called in a single-threaded fashion anyways
*/
func (sc *Sidecar) TestingMockClock(_ *testing.T, start time.Time, increment time.Duration) {
	sc.testing.mode = true
	sc.Schedule.Cadence = time.Duration(1 * time.Nanosecond) // just always set this to be sure
	sc.config.clock = func(z timezones.ZoneID) time.Time {
		start = start.Add(increment).In(z.Location())
		return start
	}
}

func (sc *Sidecar) TestingSetMaxTicks(_ *testing.T, ticks int) {
	sc.testing.mode = true
	sc.testing.maxTicks = ticks
}

func (sc *Sidecar) TestingSetDelayPerTick(_ *testing.T, d time.Duration) {
	sc.testing.mode = true
	sc.testing.delayPerTick = d
}

func (sc *Sidecar) TestingSetOnPanic(_ *testing.T, fn func(logging.LoggerShape, any)) {
	sc.testing.mode = true
	sc.config.onPanic = fn
}

func (sc *Sidecar) TestingSetLogger(_ *testing.T, l logging.LoggerShape) {
	sc.config.logger = l
}

// Runs a sidecar and wires up waitgroup stuff to ensure everything has time to fully
// exercise; this avoids going through DrainAll and needing to do global reset type stuff.
// Note: Scheduled sidecars are self-closing on the quitter channel, while unscheduled
// (eg. in-memory queues) need to have the quitter channel closed after a certain period
func (sc *Sidecar) TestingExec(t *testing.T) {
	sc.testing.mode = true
	if sc.testing.maxTicks == 0 {
		assert.FailNow(t, `Refusing test run: 'testing.maxTicks' must be set`)
		return
	}
	sc.exec()
	sc.syncs.workers.Wait()
	sc.syncs.dispatcher.Wait()
	t.Logf(`[sidecar:%s] ran to completion`, sc.Handle)
}

func TestingToTime(t *testing.T, v string) time.Time {
	ts, err := time.Parse(time.RFC3339, v)
	require.Nil(t, err, errz.ErrorStack(err))
	return ts
}
