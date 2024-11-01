package sidecars_test

import (
	"fmt"
	"pkg/sys/errz"
	"pkg/sys/logging"
	"pkg/sys/sidecars"
	"pkg/sys/testsetup"
	"pkg/sys/typed"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
kill -9 $(pgrep -f "go test")
*/
func TestMain(m *testing.M) {
	testsetup.TestingSetupDB(m, `sidecars`)
}

func enqueReturnErrOnly(_ *testing.T, car sidecars.Sidecarish, arg any) error {
	_, err := car.Enque(arg)
	return errz.Trace(err)
}

func TestSidecar(t *testing.T) {
	t.Run(`MostBasic`, func(t *testing.T) {
		t.Skip()
		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType: sidecars.RunTypeMemory,
			Handle:  t.Name(),
			Handler: func(j sidecars.JobContext) error {
				t.Logf(`RAN!`)
				time.Sleep(2 * time.Second)
				return nil
			},
		})

		go func() {
			t.Logf(`Enqueud = %s`, enqueReturnErrOnly(t, car, `first`))
			t.Logf(`Enqueud = %s`, enqueReturnErrOnly(t, car, `two`))
		}()

		car.TestingSetLogger(t, logging.NewNoOpLogger())
		car.TestingMockClock(t, time.Now(), time.Duration(1*time.Nanosecond))
		car.TestingSetMaxTicks(t, 5)
		car.TestingSetDelayPerTick(t, time.Duration(10*time.Millisecond))
		car.TestingExec(t)

		t.Logf(`Enqueud = %s`, enqueReturnErrOnly(t, car, `three`))
		t.Logf(`End of test`)
	})

	t.Run(`RecoversPanic`, func(t *testing.T) {
		t.Run(`Unscheduled(fast)`, func(t *testing.T) {
			t.Run(`MultiConcurrency`, func(t *testing.T) {

			})
		})

		t.Run(`Scheduled`, func(t *testing.T) {
			t.Run(`RunTypeStandalone`, func(t *testing.T) {
				x, completed, caught := 10, 0, 0
				car := sidecars.NewSidecar(&sidecars.Sidecar{
					RunType:  sidecars.RunTypeStandalone,
					Handle:   t.Name(),
					Schedule: &sidecars.Schedule{},
					// OnErrOrPanic: func(l logging.LoggerShape, r any) {
					// 	caught++
					// },
					Handler: func(j sidecars.JobContext) error {
						x--
						switch x {
						case 7, 3:
							panic(errz.Trace(errz.Errorf(`wtf mate`)))
						}
						completed++
						return nil
					},
				})
				car.TestingSetOnPanic(t, func(_ logging.LoggerShape, r any) {
					caught++
				})
				car.TestingSetLogger(t, logging.NewNoOpLogger())
				car.TestingMockClock(t, time.Now(), time.Duration(1*time.Nanosecond))
				car.TestingSetMaxTicks(t, 10)
				car.TestingExec(t)
				assert.Equal(t, 2, caught)
				assert.Equal(t, 8, completed)
			})

			t.Run(`WithQueuedJobs`, func(t *testing.T) {

			})
		})
	})

	/*
		Pay special attention to the test setup
	*/
	t.Run(`ShutsGracefullyAndDeclinesJobsWhileDraining`, func(t *testing.T) {
		sidecars.TestingReset(t)

		car1Received, car1Declined := []any{}, []error{}
		car2Received, car2Declined := []any{}, []error{}

		car1 := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeMemory,
			Handle:      fmt.Sprintf(`%s-1`, t.Name()),
			Concurrency: 3,
			Handler: func(j sidecars.JobContext) error {
				if j.JobValue().(int) <= 3 {
					time.Sleep(3 * time.Second)
				}
				car1Received = append(car1Received, j)
				return nil
			},
		})
		car1.TestingSetLogger(t, logging.NewNoOpLogger())

		car2 := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeMemory,
			Handle:      fmt.Sprintf(`%s-2`, t.Name()),
			Concurrency: 3,
			Handler: func(j sidecars.JobContext) error {
				if j.JobValue().(int) <= 3 {
					time.Sleep(3 * time.Second)
				}
				car2Received = append(car2Received, j)
				return nil
			},
		})
		car2.TestingSetLogger(t, logging.NewNoOpLogger())

		die := make(chan struct{}, 1)
		go func() {
			i := 0
			for {
				select {
				case <-die:
					return
				default:
					time.Sleep(1 * time.Nanosecond)
					if err := enqueReturnErrOnly(t, car1, i); err != nil {
						car1Declined = append(car1Declined, err)
					}
					if err := enqueReturnErrOnly(t, car2, i); err != nil {
						car2Declined = append(car2Declined, err)
					}
					i++
				}
			}
		}()

		// This is by no means an exhaustive test, but does ensure basic ("most of the time")
		// syncronization mechanisms don't panic: we have two sidecars that are bombarded by
		// jobs in the for loop above, that run for 100ms, drain is called, then runs for another
		// 100ms, then we print the results at the end. Note: the results will *always* be inconsistent,
		// but the important thing should be that they ALL RAN
		sidecars.Run()
		time.Sleep(100 * time.Millisecond)
		sidecars.DrainAll() // call DrainAll three times in a row to make sure no panics occur
		sidecars.DrainAll()
		sidecars.DrainAll()
		time.Sleep(100 * time.Millisecond)
		close(die)
		t.Logf(`len(car1Declined) = %d`, len(car1Declined))
		t.Logf(`len(car2Declined) = %d`, len(car2Declined))
	})

	t.Run(`DoesntDropInMemoryJobsOnPanic`, func(t *testing.T) {
		// when catching a panic, can we collect all queued items in the jobs
		// channel and propagate those to the next? (ps - that may already be the
		// case since the handler is per-go-routine of the workers)
	})

	t.Run(`EnqueuOnNonRunningSidecarDoesntPanic`, func(t *testing.T) {
		sidecars.TestingReset(t)

		car1 := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeMemory,
			Handle:      fmt.Sprintf(`%s-1`, t.Name()),
			Concurrency: 15,
			Handler: func(j sidecars.JobContext) error {
				t.Logf(`Did receive: %d`, j.JobValue().(int))
				return nil
			},
		})
		car1.TestingSetLogger(t, logging.NewNoOpLogger())

		go car1.Enque(1)
		time.Sleep(1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			car1.Enque(2)
		}()
		sidecars.Run() // @todo: why isn't this blocking? (should it be?)
		t.Logf(`test finished?`)
	})

	t.Run(`DoesntDropJobsOnShutdown`, func(t *testing.T) {
		t.Skip() // finnicky

		sidecars.TestingReset(t)

		var safeColl = struct {
			sync.Mutex
			coll []int
		}{coll: []int{}}

		car1 := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeMemory,
			Handle:      fmt.Sprintf(`%s-1`, t.Name()),
			Concurrency: 15,
			Handler: func(j sidecars.JobContext) error {
				if j.JobValue().(int)%3 == 0 {
					time.Sleep(1 * time.Second)
				}
				safeColl.Lock()
				defer safeColl.Unlock()
				safeColl.coll = append(safeColl.coll, j.JobValue().(int))
				return nil
			},
		})
		car1.TestingSetLogger(t, logging.NewNoOpLogger())

		for x := 1; x <= 46; x++ {
			go car1.Enque(x)
		}
		sidecars.Run()
		t.Logf(`about to drain`)
		sidecars.DrainAll()
		t.Logf(`drained`)
		require.Len(t, safeColl.coll, 46)
		sort.Ints(safeColl.coll)
		assert.Equal(t, safeColl.coll[0], 1)
		assert.Equal(t, safeColl.coll[len(safeColl.coll)-1], 46)
	})

	// t.Run(`DoesNotQueueIfTaskTimeExceedsCadence`, func(t *testing.T) {

	// })

	// t.Run(`GracefullyHandlesErrors`, func(t *testing.T) {
	// 	x, completed, caught := 10, 0, []error{}
	// 	car := sidecars.Register(&sidecars.Sidecar{
	// 		Handle:   t.Name(),
	// 		Schedule: &sidecars.Schedule{},
	// 		OnErrOrPanic: func(l logging.LoggerShape, r any) {
	// 			caught = append(caught, r.(error))
	// 		},
	// 		Handler: func(j sidecars.JobContext) error {
	// 			x--
	// 			switch x {
	// 			case 7, 3:
	// 				return errz.Errorf(`test return err`)
	// 			}
	// 			completed++
	// 			return nil
	// 		},
	// 	})
	// 	sidecars.TestingMockClock(t, car, time.Now(), time.Duration(1*time.Nanosecond))
	// 	sidecars.TestingSetMaxTicks(t, car, 10)
	// 	sidecars.TestingRunSidecar(t, car)
	// 	assert.Len(t, caught, 2)
	// 	assert.Equal(t, 8, completed)
	// })

	t.Run(`OnDayOfMonth`, func(t *testing.T) {
		times := []time.Time{}
		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:  sidecars.RunTypeStandalone,
			Handle:   t.Name(),
			Schedule: &sidecars.Schedule{OnDayOfMonth: 15},
			Handler: func(j sidecars.JobContext) error {
				times = append(times, j.JobValue().(time.Time))
				return nil
			},
		})

		knownClockStart := sidecars.TestingToTime(t, `2021-05-01T00:00:00Z`) // midnight
		car.TestingMockClock(t, knownClockStart, time.Duration(24*time.Hour))
		car.TestingSetMaxTicks(t, 90)
		car.TestingSetLogger(t, logging.NewNoOpLogger())
		car.TestingExec(t)

		require.Len(t, times, 3)
		assert.Equal(t, `2021-05-15`, times[0].Format(`2006-01-02`))
		assert.Equal(t, `2021-06-15`, times[1].Format(`2006-01-02`))
		assert.Equal(t, `2021-07-15`, times[2].Format(`2006-01-02`))
	})

	t.Run(`AtSpecificTimes`, func(t *testing.T) {
		times := []time.Time{}
		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType: sidecars.RunTypeStandalone,
			Handle:  t.Name(),
			Schedule: &sidecars.Schedule{
				SpecificTimes: []sidecars.SidecarTime{
					{H: 2, M: 17},
					{H: 9, M: 43},
					{H: 18, M: 22},
				},
			},
			Handler: func(j sidecars.JobContext) error {
				times = append(times, j.JobValue().(time.Time))
				return nil
			},
		})

		knownClockStart := sidecars.TestingToTime(t, `2021-05-23T00:00:00Z`) // midnight
		car.TestingMockClock(t, knownClockStart, time.Duration(1*time.Minute))
		car.TestingSetMaxTicks(t, 2880) // 1440 minutes per day, *2 (with clock using x1min)
		car.TestingSetLogger(t, logging.NewNoOpLogger())
		car.TestingExec(t)

		require.Len(t, times, 6)
		assert.Equal(t, `02:17`, times[0].Format(`15:04`))
		assert.Equal(t, `09:43`, times[1].Format(`15:04`))
		assert.Equal(t, `18:22`, times[2].Format(`15:04`))
		assert.Equal(t, `02:17`, times[3].Format(`15:04`))
		assert.Equal(t, `09:43`, times[4].Format(`15:04`))
		assert.Equal(t, `18:22`, times[5].Format(`15:04`))
	})

	t.Run(`OnDayOfMonthANDSpecificTimes`, func(t *testing.T) {
		times := []time.Time{}
		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType: sidecars.RunTypeStandalone,
			Handle:  t.Name(),
			Schedule: &sidecars.Schedule{
				OnDayOfMonth: 15,
				SpecificTimes: []sidecars.SidecarTime{
					{H: 2, M: 17},
					{H: 9, M: 43},
					{H: 18, M: 22},
				},
			},
			Handler: func(j sidecars.JobContext) error {
				times = append(times, j.JobValue().(time.Time))
				return nil
			},
		})

		knownClockStart := sidecars.TestingToTime(t, `2021-05-23T00:00:00Z`) // midnight
		car.TestingMockClock(t, knownClockStart, time.Duration(1*time.Minute))
		car.TestingSetMaxTicks(t, 130000) // 1440 minutes per day, *90-ish (with clock using x1min)
		car.TestingSetLogger(t, logging.NewNoOpLogger())
		car.TestingExec(t)

		require.Len(t, times, 9)
		assert.Equal(t, `2021-06-15T02:17:00Z`, times[0].Format(time.RFC3339))
		assert.Equal(t, `2021-06-15T09:43:00Z`, times[1].Format(time.RFC3339))
		assert.Equal(t, `2021-06-15T18:22:00Z`, times[2].Format(time.RFC3339))
		assert.Equal(t, `2021-07-15T02:17:00Z`, times[3].Format(time.RFC3339))
		assert.Equal(t, `2021-07-15T09:43:00Z`, times[4].Format(time.RFC3339))
		assert.Equal(t, `2021-07-15T18:22:00Z`, times[5].Format(time.RFC3339))
		assert.Equal(t, `2021-08-15T02:17:00Z`, times[6].Format(time.RFC3339))
		assert.Equal(t, `2021-08-15T09:43:00Z`, times[7].Format(time.RFC3339))
		assert.Equal(t, `2021-08-15T18:22:00Z`, times[8].Format(time.RFC3339))
	})
}

type (
	TestDurableRecord struct {
		ID   typed.Int
		Name typed.String
		DOB  typed.Timestamp
	}
)

func TestDurables(t *testing.T) {
	// @todo: run this test in isolation and observe that some records are
	// left in 'working' status
	t.Run(`AllTheThings`, func(t *testing.T) {
		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeDeferred,
			Concurrency: 3, //30,
			Handle:      t.Name(),
			Schedule:    &sidecars.Schedule{},
			Timeout:     1 * time.Second,
			Handler: func(j sidecars.JobContext) error {
				var record TestDurableRecord
				if err := j.UnmarshalRecordTo(&record); err != nil {
					return errz.Trace(err)
				}
				// t.Logf(`jobVal = %v`, sys.MustObjAsPrettyJSON(record))
				time.Sleep(250 * time.Millisecond)

				// test pausing long enough to timeout
				if record.ID.Int64%11 == 0 {
					time.Sleep(5 * time.Second)
					return nil // return errz.Wrap(errz.Errorf(`forced timeout`), errz.BadRequestf(`timedout!`))
				}

				// test recording record status to failed
				if record.ID.Int64%7 == 0 {
					return errz.Wrap(errz.Errorf(`forced failure`), errz.BadRequestf(`bad one!`))
				}

				return nil
			},
		})

		go func() {
			x := 1
			for {
				if x == 21 {
					break
				}
				time.Sleep(10 * time.Millisecond)
				err := enqueReturnErrOnly(t, car, TestDurableRecord{
					ID:   typed.NewInt(x),
					Name: typed.NewStringf(`lorem %d`, x),
					DOB:  typed.NewTimestamp(time.Now()),
				})
				assert.Nil(t, err, errz.ErrorStack(err))
				if err != nil {
					break
				}
				x++
			}
		}()

		car.TestingMockClock(t, time.Now(), time.Duration(1*time.Minute))
		car.TestingSetMaxTicks(t, 5)
		car.TestingSetDelayPerTick(t, time.Duration(100*time.Millisecond))
		car.TestingSetLogger(t, logging.NewNoOpLogger())
		car.TestingExec(t)
		// time.Sleep(5 * time.Second)
		t.Logf(`End of test`)
	})

	t.Run(`RealRunMechanics`, func(t *testing.T) {

	})

	t.Run(`Reduced1`, func(t *testing.T) {
		sidecars.TestingReset(t)

		carD := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeDeferred,
			Concurrency: 2, //30,
			Handle:      t.Name(),
			Schedule:    &sidecars.Schedule{},
			Timeout:     5 * time.Second,
			Handler: func(j sidecars.JobContext) error {
				var record TestDurableRecord
				if err := j.UnmarshalRecordTo(&record); err != nil {
					return errz.Trace(err)
				}
				// t.Logf(`jobVal = %v`, sys.MustObjAsPrettyJSON(record))
				// time.Sleep(250 * time.Millisecond)

				time.Sleep(2500 * time.Millisecond)
				// test pausing long enough to timeout
				if record.ID.Int64%2 == 0 {
					time.Sleep(2000 * time.Millisecond)
					return nil // return errz.Wrap(errz.Errorf(`forced timeout`), errz.BadRequestf(`timedout!`))
				}

				return nil
			},
		})

		go func() {
			carD.Enque(TestDurableRecord{ID: typed.NewInt(1)})
			carD.Enque(TestDurableRecord{ID: typed.NewInt(2)})
			carD.Enque(TestDurableRecord{ID: typed.NewInt(3)})
			time.Sleep(900 * time.Millisecond)
			carD.Enque(TestDurableRecord{ID: typed.NewInt(4)})
		}()

		carD.TestingMockClock(t, time.Now(), time.Duration(1*time.Millisecond))
		carD.TestingSetMaxTicks(t, 100000000)
		carD.TestingSetLogger(t, logging.NewNoOpLogger())

		sidecars.Run()
		time.Sleep(1 * time.Second)
		sidecars.DrainAll()

		// go func() {
		// 	x := 1
		// 	for {
		// 		if x == 21 {
		// 			break
		// 		}
		// 		time.Sleep(10 * time.Millisecond)
		// 		err := car.Enqueue(TestDurableRecord{
		// 			ID:   typed.NewInt(x),
		// 			Name: typed.NewStringf(`lorem %d`, x),
		// 			DOB:  typed.NewTimestamp(time.Now()),
		// 		})
		// 		assert.Nil(t, err, errz.ErrorStack(err))
		// 		if err != nil {
		// 			break
		// 		}
		// 		x++
		// 	}
		// }()

		// car.TestingMockClock(t, time.Now(), time.Duration(1*time.Minute))
		// car.TestingSetMaxTicks(t, 5)
		// car.TestingSetDelayPerTick(t, time.Duration(100*time.Millisecond))
		// car.TestingExec(t, false)
		// // time.Sleep(5 * time.Second)
		// t.Logf(`End of test`)
	})

	t.Run(`DeferUntil`, func(t *testing.T) {
		var receivedJobAt time.Time

		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeDeferred,
			Concurrency: 5,
			Handle:      t.Name(),
			Schedule: &sidecars.Schedule{
				DeferNewJobsFor: time.Duration(90 * time.Minute),
			},
			Handler: func(j sidecars.JobContext) error {
				receivedJobAt = j.TestingGetSidecarExecTime(t)
				return nil
			},
		})

		go func() {
			// make sure at least one cycle has passed before sending a job
			time.Sleep(1 * time.Millisecond)
			err := enqueReturnErrOnly(t, car, `defer1`)
			assert.Nil(t, err, errz.ErrorStack(err))
		}()

		car.TestingSetLogger(t, logging.NewNoOpLogger())
		// knownClockStart := sidecars.TestingToTime(t, `2021-05-01T00:00:00Z`) // midnight
		knownClockStart := time.Now()
		car.TestingMockClock(t, knownClockStart, time.Duration(1*time.Minute))
		car.TestingSetMaxTicks(t, 120) // account for mock clock incrementing the deferuntil
		car.TestingExec(t)

		assert.True(t, receivedJobAt.Sub(knownClockStart).Minutes() >= 90)
		assert.True(t, receivedJobAt.After(knownClockStart.Add(time.Duration(90*time.Minute))))
		// t.Logf(`--> kcs=%s | rja=%s`,
		// 	knownClockStart.Format(time.RFC3339),
		// 	receivedJobAt.Format(time.RFC3339),
		// )
		// t.Logf(`diff = %v`, receivedJobAt.Sub(knownClockStart).Minutes())
	})

	// the sidecar will buffer 100 jobs at a time into memory, but if there are
	// more that should be processed (say the job runs once a month), it should
	// work through all of them if there are more than 100
	// @todo: more tests re: bulk loading, especially when there are things like
	// DeferUntil set on a job
	t.Run(`DeferredJobsProcessMoreThan100`, func(t *testing.T) {
		// the test works by having 250 records (so more than gets loaded by each
		// batch) that need to be processed; having a cadence of hourly, and ensuring
		// that the exec timestamp is the same for all of them.
		threadSafe := &struct {
			sync.Mutex
			isSet    bool
			execTime time.Time
			count    int
		}{}

		car := sidecars.NewSidecar(&sidecars.Sidecar{
			RunType:     sidecars.RunTypeDeferred,
			Concurrency: 5,
			Handle:      t.Name(),
			Schedule: &sidecars.Schedule{
				Cadence: time.Duration(1 * time.Hour),
			},
			Handler: func(j sidecars.JobContext) error {
				var jv string
				if err := j.UnmarshalRecordTo(&jv); err != nil {
					return errz.Trace(err)
				}
				receivedJobAt := j.TestingGetSidecarExecTime(t)
				// t.Logf(`received %s at: %s`, jv, receivedJobAt.Format(time.RFC3339))

				threadSafe.Lock()
				defer threadSafe.Unlock()
				if !threadSafe.isSet {
					threadSafe.isSet = true
					threadSafe.execTime = receivedJobAt
				}
				if !receivedJobAt.Equal(threadSafe.execTime) {
					assert.Failf(t, `execTime mismatch with job: %s`, jv)
					return errz.Errorf(`execTime mismatch`)
				}
				threadSafe.count++

				return nil
			},
		})

		// create 250 records before executing
		for i := 1; i <= 250; i++ {
			err := enqueReturnErrOnly(t, car, fmt.Sprintf(`test-%d`, i))
			assert.Nil(t, err, errz.ErrorStack(err))
		}

		car.TestingSetLogger(t, logging.NewNoOpLogger())
		knownClockStart := sidecars.TestingToTime(t, `2020-01-01T00:00:00Z`)
		car.TestingMockClock(t, knownClockStart, time.Duration(1*time.Minute))
		car.TestingSetMaxTicks(t, 5000)
		car.TestingExec(t)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 250, threadSafe.count)

		t.Logf(`Jobs completed!`)
	})
}
