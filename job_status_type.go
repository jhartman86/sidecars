package sidecars

import (
	"database/sql/driver"
	"encoding/json"
	"pkg/sys/db"
)

const (
	JobStatusReady    = JobStatus(`ready`)
	JobStatusWorking  = JobStatus(`working`)
	JobStatusDone     = JobStatus(`done`)
	JobStatusError    = JobStatus(`error`)
	JobStatusTimedOut = JobStatus(`timed_out`)
)

type (
	JobStatus string
)

func (js JobStatus) String() string      { return string(js) }
func (js JobStatus) Eq(v JobStatus) bool { return js == v }
func (js JobStatus) Value() (driver.Value, error) {
	if len(string(js)) == 0 {
		return nil, nil
	}
	return js.String(), nil
}
func (js JobStatus) Enumerable() (def db.Enumerable) {
	def.TypeName = `alf_sys_sidecar_job_status`
	def.Values = map[string]string{
		string(JobStatusReady):    string(JobStatusReady),
		string(JobStatusWorking):  string(JobStatusWorking),
		string(JobStatusDone):     string(JobStatusDone),
		string(JobStatusError):    string(JobStatusError),
		string(JobStatusTimedOut): string(JobStatusTimedOut),
	}
	return
}
func (js JobStatus) MarshalJSON() ([]byte, error) {
	if len(js) == 0 {
		return json.Marshal(nil)
	}
	return json.Marshal(js.String())
}
