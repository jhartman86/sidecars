package sidecars

import (
	"context"
	"encoding/json"
	"pkg/sys/errz"
	"pkg/sys/logging"
	"testing"
	"time"
)

const (
	ctxValueKey  = ctxKey(`value`)
	ctxLoggerKey = ctxKey(`logger`)
)

type (
	ctxKey string

	jobContext struct {
		context.Context
		logger  logging.LoggerShape
		sidecar *Sidecar
	}

	JobContext interface {
		Done() <-chan struct{}
		JobValue() any
		Logger() logging.LoggerShape
		UnmarshalRecordTo(v any) error
		TestingGetSidecarExecTime(_ *testing.T) time.Time
	}
)

func (ctx *jobContext) JobValue() any {
	return ctx.Value(ctxValueKey)
}

func (ctx *jobContext) Logger() logging.LoggerShape {
	return ctx.logger
}

func (ctx *jobContext) TestingGetSidecarExecTime(_ *testing.T) time.Time {
	return ctx.sidecar.config.lastExec
}

func (ctx *jobContext) UnmarshalRecordTo(v any) error {
	if !ctx.sidecar.RunType.eq(RunTypeDeferred) {
		return errz.NotValidf(`Cannot UnmarshalDurableTo with RunType:%s (only valid for RunTypeDeferred)`, ctx.sidecar.RunType)
	}
	typed, ok := ctx.JobValue().(SidecarJob)
	if !ok {
		return errz.Errorf(`UnmarshalDurableTo has invalid value (not a SidecarJob)`)
	}
	if err := json.Unmarshal(typed.JobData.Raw, v); err != nil {
		return errz.Trace(err)
	}
	return nil
}
