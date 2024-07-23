package dbsync

import (
	"database/sql"
	_ "encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	// "github.com/jackc/pgproto3/v2"
	gfn "github.com/panyam/goutils/fn"
	gut "github.com/panyam/goutils/utils"
)

type StringMap = map[string]any

// Our main DBSync type keeps track of a postgres replication slot that is being consumed and synced
type DBSync struct {
	EventsProcessor EventsProcessor
	MessageHandler  MessageHandler
	Batcher         Batcher

	// The poller peeks for messages in the replication slot periodically.  This determines delay between peeks.
	DelayBetweenPeekRetries time.Duration

	// The delay between peeks is not fixed.  Instead if no messages are found a backoff is applied
	// so this delay increases until there are more messages.  This factor determins how much the
	// delay should be increased by when no messages are found on the slot.
	TimerDelayBackoffFactor float32

	// The delay between empty peeks is capped at this amount.
	MaxDelayBetweenEmptyPeeks time.Duration

	// Maximum number of messages to read and process at a time.
	MaxMessagesToRead int

	db *sql.DB

	// The replication slot we will use to subscribe to change
	// log events from for this sync
	replicationSlot string

	// The name space where dbsync specific "sync" tables tables will be created.  These will be outside user space.
	ctrlNamespace string

	// Table where we will write watermarks for this sync
	wmTableName string

	// Which publication are we tracking with this sync?
	publication string

	relnToPGTableInfo map[uint32]*PGTableInfo

	selChan chan Selection

	tableNames []string

	// A channel letting us know how many items are to be committed
	commitReqChan chan int

	upserts   map[string](map[string]StringMap)
	deletions map[string](map[string]bool)

	isRunning bool
	cmdChan   chan cmd
	wg        sync.WaitGroup
}

type cmd struct{ Name string }

// Creates a new DBSync instance from parameters obtained from environment variables.
// The environment variables looked up are:
//   - POSTGRES_NAME - Name of the Postgres DB to setup replication on
//   - POSTGRES_HOST - Host where the DB is executing
//   - POSTGRES_PORT - Port on which the DB is served from
//   - POSTGRES_USER - Admin username to connect to the postgres db to setup replication on
//   - POSTGRES_PASSWORD - Password of the admin user
//   - DBSYNC_CTRL_NAMESPACE - Name of the control namespace where dbsync will creates its auxiliary tables
//   - DBSYNC_PUBNAME - Name of the publication tracked by dbsync
//   - DBSYNC_REPLSLOT - Name of the replication slot dbsync will track
//   - DBSYNC_WM_TABLENAME - Name of the table dbsync will use to create/track watermarks on
func NewDBSync(opts ...DBSyncOpt) (d *DBSync, err error) {
	d = &DBSync{
		TimerDelayBackoffFactor:   1.5,
		DelayBetweenPeekRetries:   100 * time.Millisecond,
		MaxDelayBetweenEmptyPeeks: 60 * time.Second,
		MaxMessagesToRead:         8192,
	}
	d.selChan = make(chan Selection)
	for _, opt := range opts {
		err = opt(d)
		if err != nil {
			return
		}
	}

	if d.ctrlNamespace == "" {
		d.ctrlNamespace = gut.GetEnvOrDefault("DBSYNC_CTRL_NAMESPACE", DEFAULT_DBSYNC_CTRL_NAMESPACE, true)
	}
	if d.wmTableName == "" {
		d.wmTableName = gut.GetEnvOrDefault("DBSYNC_WM_TABLENAME", DEFAULT_DBSYNC_WM_TABLENAME, true)
	}
	if d.publication == "" {
		d.publication = gut.GetEnvOrDefault("DBSYNC_PUBNAME", DEFAULT_DBSYNC_PUBNAME, true)
	}
	if d.replicationSlot == "" {
		d.replicationSlot = gut.GetEnvOrDefault("DBSYNC_REPLSLOT", DEFAULT_DBSYNC_REPLSLOT, true)
	}

	// Load a default DB if one not found
	if d.db == nil {
		dbconnstr := PGConnStringFromEnv()
		log.Println("Connecting to db: ", dbconnstr)
		d.db, err = sql.Open("postgres", dbconnstr)
	}

	if err == nil {
		// Create publications etc here otherwise Setup will fail
		err = d.setup()
	}
	return
}

// Tells if the poller is running or stopped.
func (d *DBSync) IsRunning() bool {
	return d.isRunning
}

// Signals to the poller to stop consuming messages from the replication slot
func (d *DBSync) Stop() {
	d.cmdChan <- cmd{Name: "stop"}
	d.wg.Wait()
}

func (d *DBSync) Start() {
	if d.isRunning {
		return
	}
	d.isRunning = true
	log.Println("DBSync Started")
	d.wg.Add(1)
	d.commitReqChan = make(chan int)
	timerDelay := 0 * time.Second
	readTimer := time.NewTimer(0)
	defer func() {
		d.isRunning = false
		close(d.commitReqChan)
		if !readTimer.Stop() {
			<-readTimer.C
		}
		d.commitReqChan = nil
		d.wg.Done()
		log.Println("DBSync Stopped")
	}()
	for {
		select {
		case selReq := <-d.selChan:
			selReq.Execute()
			// Now get all the entries from this selection and
			// process them first
			break
		case cmd := <-d.cmdChan:
			if cmd.Name == "stop" {
				// clean up and stop
				return
			} else if cmd.Name == "resetreadtimer" {
				readTimer.Reset(0)
			} else {
				log.Println("Invalid command: ", cmd)
			}
		case <-readTimer.C:
			msgs, err := d.GetMessages(d.MaxMessagesToRead, false, nil)
			if err != nil {
				d.EventsProcessor(nil, err)
				return
			}
			if len(msgs) == 0 {
				// Nothing found - try again after a delay
				slog.Debug("Timer delay: ", "timerDelay", timerDelay)
				if timerDelay == 0 {
					timerDelay += d.DelayBetweenPeekRetries
				} else {
					timerDelay = (3 * timerDelay) / 2
				}
				if timerDelay > d.MaxDelayBetweenEmptyPeeks {
					timerDelay = d.MaxDelayBetweenEmptyPeeks
				}
			} else {
				timerDelay = 0

				// Here we should process the messages so we can tell the DB
				numProcessed, stop := d.EventsProcessor(msgs, err)
				err = d.Forward(numProcessed)
				if err != nil {
					panic(err)
				}
				if stop {
					return
				}
			}
			readTimer = time.NewTimer(timerDelay)
		}
	}
}

// Returns the info about a table given its relation ID
func (p *DBSync) GetTableInfo(relationID uint32) *PGTableInfo {
	if p.relnToPGTableInfo == nil {
		p.relnToPGTableInfo = make(map[uint32]*PGTableInfo)
	}
	tableinfo, ok := p.relnToPGTableInfo[relationID]
	if !ok {
		tableinfo = &PGTableInfo{
			RelationID: relationID,
			ColInfo:    make(map[string]*PGColumnInfo),
		}
		p.relnToPGTableInfo[relationID] = tableinfo
	}
	return tableinfo
}

// Queries the DB for the latest schema of a given relation and stores it
func (p *DBSync) RefreshTableInfo(relationID uint32, namespace string, table_name string) (tableInfo *PGTableInfo, err error) {
	field_info_query := fmt.Sprintf(`SELECT table_schema, table_name, column_name, ordinal_position, data_type, table_catalog from information_schema.columns WHERE table_schema = '%s' and table_name = '%s' ;`, namespace, table_name)
	log.Println("Query for field types: ", field_info_query)
	rows, err := p.db.Query(field_info_query)
	if err != nil {
		log.Println("Error getting table info: ", err)
		return nil, err
	}
	defer rows.Close()
	tableInfo = p.GetTableInfo(relationID)
	for rows.Next() {
		var col PGColumnInfo
		if err := rows.Scan(&col.Namespace, &col.TableName, &col.ColumnName, &col.OrdinalPosition, &col.ColumnType, &col.DBName); err != nil {
			log.Println("Could not scan row: ", err)
		} else {
			if colinfo, ok := tableInfo.ColInfo[col.ColumnName]; !ok {
				tableInfo.ColInfo[col.ColumnName] = &col
			} else {
				colinfo.DBName = col.DBName
				colinfo.Namespace = col.Namespace
				colinfo.TableName = col.TableName
				colinfo.ColumnName = col.ColumnName
				colinfo.ColumnType = col.ColumnType
				colinfo.OrdinalPosition = col.OrdinalPosition
			}
		}
	}
	return
}

// Sets up the replication slot with our auxiliary namespace, watermark table, publications and replication slots
func (d *DBSync) setup() (err error) {
	if d.db == nil {
		log.Fatal("DB is not setup")
	}
	err = d.ensureNamespace()

	if err == nil {
		err = d.ensurewmTable()
	}

	if err == nil {
		err = d.registerWithPublication()
	}

	if err == nil {
		err = d.setupReplicationSlots()
	}
	return
}

// Returns the underlying sql.DB instance being tracked
func (p *DBSync) DB() *sql.DB {
	return p.db
}

func (d *DBSync) MarkUpdated(doctype string, docid string, doc StringMap) {
	d.upserts[doctype][docid] = doc
	if d.deletions[doctype] != nil && d.deletions[doctype][docid] {
		d.deletions[doctype][docid] = false
	}
}

func (d *DBSync) MarkDeleted(doctype string, docid string) {
	if d.upserts[doctype] != nil && d.upserts[doctype][docid] != nil {
		delete(d.upserts[doctype], docid)
	}
	if d.deletions[doctype] == nil {
		d.deletions[doctype] = make(map[string]bool)
	}
	d.deletions[doctype][docid] = true
}

// Returns numMessages number of events at the front of the replication slot (queue).  If consume parameter is set, then the offset
// is automatically forwarded, otherwise repeated calls to this method will simply returned "peeked" messages.
func (p *DBSync) GetMessages(numMessages int, consume bool, out []PGMSG) (msgs []PGMSG, err error) {
	msgs = out
	changesfuncname := "pg_logical_slot_peek_binary_changes"
	if consume {
		changesfuncname = "pg_logical_slot_get_binary_changes"
	}
	q := fmt.Sprintf(`select * from %s(
					'%s', NULL, %d,
					'publication_names', '%s',
					'proto_version', '1') ;`,
		changesfuncname, p.replicationSlot, numMessages, p.publication)
	rows, err := p.db.Query(q)
	if err != nil {
		log.Println("SELECT NAMESPACE ERROR: ", err)
		return nil, err
	}

	for rows.Next() {
		var msg PGMSG
		err = rows.Scan(&msg.LSN, &msg.Xid, &msg.Data)
		if err != nil {
			log.Println("Error scanning change: ", err)
			return
		}
		msgs = append(msgs, msg)
	}
	return
}

// Forwards the message offset on the replication slot.  Typically GetMessages is called to peek N messages.  Then after those messages are processed the offset is forwarded to ensure at-least once processing of messages.
func (p *DBSync) Forward(nummsgs int) error {
	changesfuncname := "pg_logical_slot_get_binary_changes"
	q := fmt.Sprintf(`select * from %s('%s', NULL, %d,
					'publication_names', '%s',
					'proto_version', '1') ;`,
		changesfuncname, p.replicationSlot, nummsgs, p.publication)
	rows, err := p.db.Query(q)
	if err != nil {
		log.Println("SELECT NAMESPACE ERROR: ", err)
		return err
	}
	// We dont actually need the results
	defer rows.Close()

	// Now update our peek offset
	// peekOffset tells where to do the next "limit" function from
	return nil
}

func (p *DBSync) ensureNamespace() (err error) {
	rows, err := p.db.Query("SELECT * from pg_catalog.pg_namespace where nspname = $1", p.ctrlNamespace)
	if err != nil {
		log.Println("SELECT NAMESPACE ERROR: ", err)
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		// Name space does not exist so create it
		create_schema_query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION CURRENT_USER", p.ctrlNamespace)
		_, err := p.db.Exec(create_schema_query)
		if err != nil {
			log.Println("CREATE SCHEMA ERROR: ", err)
			return err
		}
	}

	return nil
}

func (p *DBSync) ensurewmTable() (err error) {
	// Check if our WM table exists
	rows, err := p.db.Query("SELECT relname, relnamespace, reltype FROM pg_catalog.pg_class WHERE relname = $1 AND relkind = 'r'", p.wmTableName)
	if err != nil {
		log.Println("Get wmTable Error: ", err)
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		// create this table
		create_wmtable_query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
				selectionid varchar(50) PRIMARY KEY,
				low_wm varchar(50),
				high_wm varchar(50)
			)`, p.ctrlNamespace, p.wmTableName)
		_, err = p.db.Exec(create_wmtable_query)
		if err != nil {
			log.Println("wmTable Creation Error: ", err)
			return
		}
	}

	return nil
}

func (p *DBSync) registerWithPublication() error {
	// Now ensure our WM table is assigned to the publication
	q := fmt.Sprintf(`select pubname from pg_publication_tables where schemaname = '%s' and tablename = '%s'`, p.ctrlNamespace, p.wmTableName)
	rows, err := p.db.Query(q)
	if err != nil {
		log.Println("Could not query pb_publication_tables: ", err)
		return err
	}
	defer rows.Close()
	if rows.Next() {
		// There is a row - so make sure our pubname matches the given publication
		// if it doesnt then it means we have an error
		var pubname string
		if err := rows.Scan(&pubname); err != nil {
			log.Println("Error scanning pubname: ", err)
			return err
		}
		if pubname != p.publication {
			return fmt.Errorf("table %s.%s is already assigned to Publication '%s'", p.ctrlNamespace, p.wmTableName, pubname)
		}
	} else {
		// our table is not part of the publication so add to it
		alterpub := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s.%s`, p.publication, p.ctrlNamespace, p.wmTableName)
		_, err := p.db.Exec(alterpub)
		if err != nil {
			log.Println("ALTER PUBLICATION Error : ", err)
			createpubsql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE table1, table2, ..., tableN ;", p.publication)
			log.Printf("Did you create the publication?  Try: %s", createpubsql)
			return err
		}
	}
	return nil
}

/**
 * Create our replication slots and prepare it to be ready for peek/geting events
 * from our publication.  If a slot already exists, then ensures it is a pgoutput type
 */
func (p *DBSync) setupReplicationSlots() error {
	q := fmt.Sprintf(`SELECT slot_name, plugin, slot_type, restart_lsn, confirmed_flush_lsn
			FROM pg_replication_slots
			WHERE slot_name = '%s'`, p.replicationSlot)
	rows, err := p.db.Query(q)
	if err != nil {
		log.Println("Error Getting Replication Slots: ", err)
		return err
	}
	defer rows.Close()
	if rows.Next() {
		var slot_name string
		var plugin string
		var slot_type string
		var restart_lsn string
		var confirmed_flush_lsn string

		if err := rows.Scan(&slot_name, &plugin, &slot_type, &restart_lsn, &confirmed_flush_lsn); err != nil {
			log.Println("Error scanning slot_name, plugin, plot_type: ", err)
			return err
		}
		if slot_name != p.replicationSlot {
			return fmt.Errorf("replication slot invalid: %s", p.replicationSlot)
		}
		if plugin != "pgoutput" {
			return fmt.Errorf("invalid plugin (%s).  Only 'pgoutput' supported", plugin)
		}
		if slot_type != "logical" {
			return fmt.Errorf("invalid replication (%s).  Only 'logical' supported", slot_type)
		}
	} else {
		// Create it
		q := fmt.Sprintf(`SELECT * FROM pg_create_logical_replication_slot('%s', 'pgoutput', false, true);`, p.replicationSlot)
		rows2, err := p.db.Query(q)
		if err != nil {
			log.Println("SELECT NAMESPACE ERROR: ", err)
			return err
		}
		defer rows2.Close()
		if !rows2.Next() {
			return fmt.Errorf("pg_create_logical_replication_slot returned no rows")
		}
	}
	return nil
}

func (d *DBSync) processBatchMessages(msgs []PGMSG, err error) (numProcessed int, stop bool) {
	if err != nil {
		log.Println("Error processing messsages: ", err)
		return 0, false
	}
	for i, rawmsg := range msgs {
		err := d.MessageHandler.HandleMessage(i, &rawmsg)
		// Handle batch deletions
		if err == ErrStopProcessingMessages {
			break
		} else if err != nil {
			log.Println("Error handling message: ", i, err)
		}
	}
	if d.Batcher != nil {
		for doctype, docs := range d.deletions {
			docids := gfn.MapKeys(docs)
			if len(docids) > 0 {
				res, err := d.Batcher.BatchDelete(doctype, docids)
				if err != nil {
					log.Println("Error deleting: ", doctype, docids, err, res)
				}
			}
		}

		// And batch inserts too
		for doctype, docmaps := range d.upserts {
			if len(docmaps) > 0 {
				docs := gfn.MapValues(docmaps)
				res, err := d.Batcher.BatchUpsert(doctype, docs)
				if err != nil {
					log.Println("Batch Upsert Error: ", doctype, err, res)
				}
			}
		}
	}

	// Reset this stuff
	d.upserts = make(map[string]map[string]StringMap)
	d.deletions = make(map[string]map[string]bool)
	if d.MessageHandler.LastCommit() > 0 {
		return d.MessageHandler.LastCommit() + 1, false
	} else {
		return len(msgs), false
	}
}
