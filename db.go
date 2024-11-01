package sidecars

import "pkg/sys/db"

func init() {
	db.Register(db.Migrator{
		AutoTables: []any{
			SidecarJob{},
		},
		Enumerables: []interface{ Enumerable() db.Enumerable }{
			JobStatus(``),
		},
		Indices: []db.Index{
			{Table: SidecarJob{}, Unique: false, Cols: []string{`created_at`}},
			{Table: SidecarJob{}, Unique: false, Cols: []string{`handle`}},
		},
	})
}
