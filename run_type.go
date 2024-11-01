package sidecars

const (
	// in-memory means the sidecar *can* receive jobs, and those get passed
	// through directly to a worker immediately (in-memory), without any persistence
	RunTypeMemory = `memory`
	// deferred menas the sidecar *can* receive jobs, and those get durably persisted
	// when passed, then the worker runs on a scheduled interval to process those jobs
	RunTypeDeferred = `deferred`
	// standalone means its a scheduled sidecar (at least 1 min intervals), that CANNOT
	// be sent jobs; instead, whenever a standalone executes on the given cadence, it just
	// does its' thing, fully self-contained
	RunTypeStandalone = `standalone`
)

type runType string

func (rt runType) eq(v runType) bool { return rt == v }

func (rt runType) valid() bool {
	switch rt {
	case RunTypeMemory, RunTypeDeferred, RunTypeStandalone:
		return true
	}
	return false
}
