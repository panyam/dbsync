package dbsync

import "database/sql"

type SyncerOpt func(*Syncer) error

// Sets name of the control namespace used by dbsync internally
func WithCtrlNamespace(nsname string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.ctrlNamespace = nsname
		return
	}
}

// Sets name of the watermark table to be used
func WithWMTable(wmtablename string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.wmTableName = wmtablename
		return
	}
}

// Sets the replication slot tracked by dbsync
func WithReplicationSlot(slotname string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.replicationSlot = slotname
		return
	}
}

// Sets the publication name tracked by dbsync
func WithPublication(pubname string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.publication = pubname
		return
	}
}

// Sets the table names to automatically create a publication for if the user does not want to create the publication manually
func ForTables(tableNames ...string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.tableNames = tableNames
		return
	}
}

// Sets the connection string for the postsgres database to be synced
func WithPGConnStr(connstr string) SyncerOpt {
	return func(d *Syncer) (err error) {
		d.db, err = sql.Open("postgres", connstr)
		if err != nil {
			panic(err)
		}
		return
	}
}
