package sidecars

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type (
	jobData struct {
		Data any
		Raw  []byte // @todo: unexport
	}
)

func toJobData(v interface{}) jobData {
	return jobData{Data: v}
}

func (j jobData) IsEmpty() bool {
	return j.Data == nil
}

// func (j jobData) UnmarshalTo(v any) error {
// 	return errz.Trace(json.Unmarshal(j.Raw, v))
// }

// Scanning a value from the database: ingests a raw byte
// array and turns into a usable jobDataBGeneric
func (j *jobData) Scan(value interface{}) error {
	if value == nil {
		(*j) = jobData{}
		return nil
	}
	t, ok := value.([]byte)
	if !ok {
		return fmt.Errorf(`jobData conversion error`)
	}
	j.Raw = t
	// @todo: on read, store only the raw, skip unmarshalling to Data
	return json.Unmarshal(t, &j.Data)
}

// When sending to the database, we want to marshal to a JSON
// byte array for storage
func (j jobData) Value() (driver.Value, error) {
	if j.Data == nil {
		return nil, nil
	}
	return json.Marshal(j.Data)
}

// Marshal the current state of the JSON to a byte array
// representing the JSON
func (j jobData) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.Data)
}

// Take the raw byte array and convert into the JSON
func (j *jobData) UnmarshalJSON(data []byte) (err error) {
	err = json.Unmarshal(data, &j.Data)
	return
}
