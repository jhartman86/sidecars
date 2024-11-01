package sidecars

import (
	"database/sql"
	"pkg/sys/db"
	"pkg/sys/errz"
	"pkg/sys/typed"
	"time"
)

const (
	batchLoadSize = 100
)

type (
	SidecarJob struct {
		ID         typed.UUID      `json:"id" gorm:"primaryKey;type:uuid;default:alf_fn_generate_ulid_uuid()"`
		CreatedAt  typed.Timestamp `json:"createdAt" gorm:"type:timestamp;default:clock_timestamp();not null;"`
		UpdatedAt  typed.Timestamp `json:"updatedAt" gorm:"type:timestamp;default:clock_timestamp();not null;"`
		Handle     typed.String    `json:"handle" gorm:"type:varchar;not null;"`
		Status     JobStatus       `json:"status" gorm:"type:alf_sys_sidecar_job_status;default:ready;not null;"`
		DeferUntil typed.Timestamp `json:"deferUntil" gorm:"type:timestamp;"`
		JobData    jobData         `json:"jobData" gorm:"type:jsonb;not null;"`
		ErrorData  typed.String    `json:"errorData" gorm:"type:text;"`
	}
)

func (SidecarJob) TableName() string { return `sys_sidecar_job` }

/*
loadReadySidecarJobs finds jobs that are in ready status, marks them all
as 'working' status, then returns them (in groups of 100). It also sorts
by created_at asc so it'll take the oldest jobs first.

note: n (now) time is injectable so that it can be lied about during test
*/
func loadReadySidecarJobs(handle string, n time.Time) ([]SidecarJob, error) {
	jobs := []SidecarJob{}
	if err := db.Conn().Direct().Raw(`
		WITH ready AS (
			SELECT * FROM sys_sidecar_job
			WHERE 
				handle = @handle 
				AND status = @statusReady
				AND (defer_until IS NULL OR defer_until <= @timeNow)
			ORDER BY created_at ASC LIMIT @batchLoadSize
		)
		UPDATE sys_sidecar_job SET status = @statusWorking
		FROM ready WHERE sys_sidecar_job.id = ready.id
		RETURNING *`,
		sql.Named(`handle`, handle),
		sql.Named(`statusReady`, JobStatusReady),
		sql.Named(`statusWorking`, JobStatusWorking),
		sql.Named(`timeNow`, n),
		sql.Named(`batchLoadSize`, batchLoadSize),
	).Scan(&jobs).Error; err != nil {
		return nil, errz.Trace(err)
	}
	return jobs, nil
}

func (scj SidecarJob) recordStatus(js JobStatus, errData ...typed.String) error {
	if !scj.ID.Valid {
		return errz.NotValidf(`Cannot persist record status when ID is unset`)
	}
	scj.Status = js
	fields := []string{`status`}
	for i := range errData {
		scj.ErrorData = errData[i]
		fields = append(fields, `error_data`)
		break
	}
	if err := db.Conn().Direct().
		Model(&SidecarJob{}).
		Select(fields).
		Omit(`updated_at`). // auto handled db trigger
		Where(`id = ?`, scj.ID).
		Updates(&scj).Error; err != nil {
		return errz.Trace(err)
	}
	return nil
}
