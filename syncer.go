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
	"github.com/jackc/pglogrepl"
	gfn "github.com/panyam/goutils/fn"
	gut "github.com/panyam/goutils/utils"
)

type StringMap = map[string]any

type selection interface {
	ID() string

	// Gets all items currently remaining in the selection
	// TODO - Should we allow iteration on this in case we
	// are ok to have a *really* large dataset as part of this
	Items() map[any]DBItem

	// Get the value of an item in this selection given its key.
	GetItem(key any) (value any, exists bool)

	// Removes an item from this collection
	RemoveItem(key any) any

	// Clears all items from this selection to release any storage needed
	Clear() bool
}

// Our main Syncer type keeps track of a postgres replication slot that is being consumed and synced
type Syncer struct {
	MessageHandler MessageHandler
	Batcher        Batcher

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

	refreshChan      chan string
	currSelection    selection
	enteredWatermark bool

	tableNames []string

	// A channel letting us know how many items are to be committed
	commitReqChan chan int

	// Marks when the last begin and last commit messages were
	lastBegin  int
	lastCommit int
	relnCache  map[uint32]*pglogrepl.RelationMessage

	upserts   map[string](map[string]StringMap)
	deletions map[string](map[string]bool)

	isRunning bool
	cmdChan   chan cmd
	wg        sync.WaitGroup
}

type cmd struct{ Name string }

// Creates a new Syncer instance from parameters obtained from environment variables.
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
func NewSyncer(opts ...SyncerOpt) (d *Syncer, err error) {
	d = &Syncer{
		TimerDelayBackoffFactor:   1.5,
		DelayBetweenPeekRetries:   100 * time.Millisecond,
		MaxDelayBetweenEmptyPeeks: 60 * time.Second,
		MaxMessagesToRead:         8192,
	}
	d.refreshChan = make(chan string)
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
func (d *Syncer) IsRunning() bool {
	return d.isRunning
}

// Signals to the poller to stop consuming messages from the replication slot
func (d *Syncer) Stop() {
	d.cmdChan <- cmd{Name: "stop"}
	d.wg.Wait()
}

// Kicks off a refresh of the records returned given by the select query.
func (d *Syncer) Refresh(selectQuery string) bool {
	// write the Low water mark into the wm table
	if d.currSelection != nil {
		log.Println("In the middle of another selection.  Try again when this is complete")
		return false
	}
	d.currSelection = NewInMemSelection()

	d.refreshChan <- selectQuery
	return true
}

// Starts the syncer loop
func (d *Syncer) Start() {
	if d.isRunning {
		return
	}
	d.isRunning = true
	log.Println("Syncer Started")
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
		log.Println("Syncer Stopped")
	}()
	for {
		select {
		case selectQuery := <-d.refreshChan:
			d.processRefreshQuery(selectQuery)
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
			// TODO - Should we cache what is read so that we dont repeat reads where offset is only forwarded partially?
			msgs, err := d.GetMessages(d.MaxMessagesToRead, false, nil)
			if err != nil {
				log.Println("Error processing messsages: ", err)
				return
			}
			if len(msgs) == 0 {
				// Nothing found - try again after a delay
				timerDelay = d.incrementTimerDelay(timerDelay)
			} else {
				timerDelay = 0

				// Here we should process the messages so we can tell the DB
				numProcessed, stop := d.processMessages(msgs, err)
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
func (p *Syncer) GetTableInfo(relationID uint32) *PGTableInfo {
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
func (p *Syncer) RefreshTableInfo(relationID uint32, namespace string, table_name string) (tableInfo *PGTableInfo, err error) {
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

// Returns the underlying sql.DB instance being tracked
func (p *Syncer) DB() *sql.DB {
	return p.db
}

func (d *Syncer) MarkUpdated(doctype string, docid string, doc StringMap) {
	d.upserts[doctype][docid] = doc
	if d.deletions[doctype] != nil && d.deletions[doctype][docid] {
		d.deletions[doctype][docid] = false
	}
}

func (d *Syncer) MarkDeleted(doctype string, docid string) {
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
func (p *Syncer) GetMessages(numMessages int, consume bool, out []PGMSG) (msgs []PGMSG, err error) {
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
func (p *Syncer) Forward(nummsgs int) error {
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

func (d *Syncer) UpdateRelation(reln *pglogrepl.RelationMessage) *PGTableInfo {
	if d.relnCache == nil {
		d.relnCache = make(map[uint32]*pglogrepl.RelationMessage)
	}
	d.relnCache[reln.RelationID] = reln

	// Query to get info on pkeys
	tableInfo, _ := d.RefreshTableInfo(reln.RelationID, reln.Namespace, reln.RelationName)
	return tableInfo

	/*
			getpkeyquer := fmt.Sprintf(`SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
								FROM   pg_index i
								JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
								WHERE  i.indrelid = 'tablename'::regclass AND i.indisprimary`, reln.Namespace, reln.RelationName)
		log.Println("Query for pkey: ", getpkeyquer)
	*/
}

func (d *Syncer) GetRelation(relationId uint32) *pglogrepl.RelationMessage {
	if d.relnCache == nil {
		d.relnCache = make(map[uint32]*pglogrepl.RelationMessage)
	}
	reln, ok := d.relnCache[relationId]
	if !ok {
		panic("Could not find relation - Need to query DB manually")
	}
	return reln
}

func (s *Syncer) MessageToMap(msg *pglogrepl.TupleData, reln *pglogrepl.RelationMessage) (pkey string, out map[string]any, errors map[string]error) {
	msgcols := msg.Columns
	relcols := reln.Columns
	if len(msgcols) != len(relcols) {
		log.Printf("Msg cols (%d) and Rel cols (%d) dont match", len(msgcols), len(relcols))
	}
	// fullschema := fmt.Sprintf("%s.%s", reln.Namespace, reln.RelationName)
	// log.Printf("Namespace: %s, RelName: %s, FullSchema: %s", reln.Namespace, reln.RelationName, fullschema)
	pkey = "id"
	out = make(map[string]any)
	tableinfo := s.GetTableInfo(reln.RelationID)
	for i, col := range reln.Columns {
		val := msgcols[i]
		colinfo := tableinfo.ColInfo[col.Name]
		// log.Println("Cols: ", i, col.Name, val, colinfo)
		var err error
		if val.DataType == pglogrepl.TupleDataTypeText {
			out[col.Name], err = colinfo.DecodeText(val.Data)
		} else if val.DataType == pglogrepl.TupleDataTypeBinary {
			out[col.Name], err = colinfo.DecodeBytes(val.Data)
		}
		if err != nil {
			if errors == nil {
				errors = make(map[string]error)
			}
			errors[col.Name] = err
		}
	}
	return
}

// Sets up the replication slot with our auxiliary namespace, watermark table, publications and replication slots
func (d *Syncer) setup() (err error) {
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

func (p *Syncer) ensureNamespace() (err error) {
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

func (p *Syncer) ensurewmTable() (err error) {
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
				wm_type varchar(10)
			)`, p.ctrlNamespace, p.wmTableName)
		_, err = p.db.Exec(create_wmtable_query)
		if err != nil {
			log.Println("wmTable Creation Error: ", err)
			return
		}
	}

	return nil
}

func (p *Syncer) registerWithPublication() error {
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
func (p *Syncer) setupReplicationSlots() error {
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

func (d *Syncer) processRefreshQuery(selectQuery string) {
	defer func() {
		d.currSelection = nil
	}()
	if d.currSelection == nil {
		log.Println("Called to process a refresh but selection is not sest.")
		return
	}

	lowWmQuery := fmt.Sprintf(`INSERT INTO %s.%s (selectionid, wm_type) VALUES (?, 'low')`, d.ctrlNamespace, d.wmTableName)
	_, err := d.db.Exec(lowWmQuery, d.currSelection.ID())
	if err != nil {
		log.Println("Error creating low water mark: ", err)
		return
	}

	// Now perform a selection and set it
	_, err = d.db.Exec(selectQuery)
	if err != nil {
		log.Println("Error executing selection: ", err)
		return
	}

	highWmQuery := fmt.Sprintf(`INSERT INTO %s.%s (selectionid, wm_type) VALUES (?, 'high')`, d.ctrlNamespace, d.wmTableName)
	_, err = d.db.Exec(highWmQuery, d.currSelection.ID())
	if err != nil {
		log.Println("Error creating high water mark: ", err)
		return
	}
}

func (d *Syncer) incrementTimerDelay(timerDelay time.Duration) time.Duration {
	slog.Debug("Timer delay: ", "timerDelay", timerDelay)
	if timerDelay == 0 {
		timerDelay += d.DelayBetweenPeekRetries
	} else {
		timerDelay = (3 * timerDelay) / 2
	}
	if timerDelay > d.MaxDelayBetweenEmptyPeeks {
		timerDelay = d.MaxDelayBetweenEmptyPeeks
	}
	return timerDelay
}

// Called by dbsyncer to process messages in batch
func (d *Syncer) processMessages(msgs []PGMSG, err error) (numProcessed int, stop bool) {
	if err != nil {
		log.Println("Error processing messsages: ", err)
		return 0, false
	}
	for i, rawmsg := range msgs {
		err := d.processMessage(i, &rawmsg)
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

	// Reset the batch collections
	d.upserts = make(map[string]map[string]StringMap)
	d.deletions = make(map[string]map[string]bool)
	if d.lastCommit > 0 {
		return d.lastCommit + 1, false
	} else {
		return len(msgs), false
	}
}

// Called to process each individual message
func (d *Syncer) processMessage(idx int, rawmsg *PGMSG) (err error) {
	msgtype := rawmsg.Data[0]
	switch msgtype {
	case 'B':
		d.lastBegin = idx
		var msg pglogrepl.BeginMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		return d.MessageHandler.HandleBeginMessage(idx, &msg)
	case 'C':
		d.lastCommit = idx
		var msg pglogrepl.CommitMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		return d.MessageHandler.HandleCommitMessage(idx, &msg)
	case 'R':
		var msg pglogrepl.RelationMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		// TODO - Cache this so we arent doing this again and again
		tableInfo := d.UpdateRelation(&msg)
		return d.MessageHandler.HandleRelationMessage(idx, &msg, tableInfo)
	case 'D':
		var msg pglogrepl.DeleteMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		reln := d.GetRelation(msg.RelationID)
		tableinfo := d.GetTableInfo(reln.RelationID)
		return d.MessageHandler.HandleDeleteMessage(idx, &msg, reln, tableinfo)
	case 'I':
		var msg pglogrepl.InsertMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		reln := d.GetRelation(msg.RelationID)
		tableinfo := d.GetTableInfo(reln.RelationID)
		// check for low watermark message - Note this can move around dependign on how we chose to create the watermarks
		// - ie whether as inserts, updates or deletes etc
		if false {
			if d.currSelection == nil {
				log.Println("Selection is nil - water marks are useless")
			} else {
				d.enteredWatermark = true
			}
			return
		} else {
			return d.MessageHandler.HandleInsertMessage(idx, &msg, reln, tableinfo)
		}
	case 'U':
		var msg pglogrepl.UpdateMessage
		if err = msg.Decode(rawmsg.Data[1:]); err != nil {
			return
		}
		reln := d.GetRelation(msg.RelationID)
		tableinfo := d.GetTableInfo(reln.RelationID)
		// check for high watermark message - Note this can move around dependign on how we chose to create the watermarks
		// - ie whether as inserts, updates or deletes etc
		if false {
			// we have a selection we are good to go
			d.enteredWatermark = false

			// We just exited the watermark - so process items in the selection first
			if d.currSelection != nil {
				for k, v := range d.currSelection.Items() {
					err = d.MessageHandler.HandleRefresh(k, v)
					// Handle batch deletions
					if err == ErrStopProcessingMessages {
						break
					} else if err != nil {
						log.Println("Error handling message: ", idx, err)
					}
				}
			}
			return
		} else {
			return d.MessageHandler.HandleUpdateMessage(idx, &msg, reln, tableinfo)
		}
	default:
		log.Println(fmt.Sprintf("Processing Messages (%c): ", msgtype), rawmsg)
		panic(fmt.Errorf("invalid Message Type: %c", msgtype))
	}
}
