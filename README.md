
# DBSync

A simple change data capture (CDC) processing library for postgres based on [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b).

## Problem Statement

Popular databases emit events when the data they store are mutated in someway (eg created, updated, deleted etc).  These events are popularly knows as change data capture (CDC) events.  Postgres - a very popular open source database vendor is not a stranger to CDC.  It provides CDC through a process [Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) using [Replication Slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS).  Replication slots are the queue/stream of changes that are stored and can be read and replayed by a consumer in the same order as they were made.  The basic concept is something like:

```
[ Source DB]  ---> [ one or more Replication Slots ]  ---> [ Reader/Poller ] ----> [ Processed Output ]
```

* The source DB emits change events via replication slots (RS) which are persisted on disk
* The reader polls/reads the change events from the slot.
* Each replication slot has an "offset" associated which is a pointer to the last unprocessed event (as indicated by the reader).
* The reader once it processes a event can "forward" the offset to point to the next event.   Once the offset is updated, events "before" this offset cannot be read again.
* If the reader forwards the offset before processing events then we have at-most-once guarantees on event processing.  If the reader crashes after forwarding the offset but before processing it then the events are lost.
* If the reader forwards the offset AFTER processing events then we have at-least-once guarantees on event processing.  If the reader crashes after successfully processing a event but before forwarding the offset then the reader is suscepting to reading the same event again and re-processing it.
* Exactly once processing is left to the reader (of this doc) and can never be (easily) guaranteed.

This kind of processing of change events has several practical uses:

1. Data Replication/Synchronization: With CDC replication of databases across different environments is enabled (eg production <-> data warehouse, creation of secondry indexes etc).  This can also include use cases like incremental backups - where capturing only the changes since the last backup would  reduce storage requirements and backup time.
Data Recovery: Facilitating point-in-time recovery by replaying captured changes to restore a database to a specific state.
2. ETL (Extract, Transform, Load): When ETL pipelines are integrated with CDC, this can ensure target datastores (data warehouses or data lakes) are updated in (near) real-time.  This results in fresher and more relevant data for downstream uses
3.  Event Sourcing: Event driven architectures can be enabled by using CDC to create event streams.
4. Auditing and Compliance: A detailed log of chagnes can also help with regulatory compliance and audit requirements.  Tracking the origin and transformation of data over time to understand its lifecycle and maintain data quality is also possible with CDC.
6.  Real-Time Analytics: CDC can help provide up-to-date data for real-time analytics and dashboards. This would help businesses to make informed decisions based on the latest information.
7. And others like incremental monitoring/alerting, updates to ML/AI models with latest data, and more.

### Challenges

While the above setup is intuitive there are a few consistency and timeliness challenges:

1. Replication slots only capture changes from the point they were "setup".  So if the RS was not setup at the time of the database's creation the reader/poller will only have access to partial change events.   If such a system is used to replicate data only partial replication is possible.
2. In order to ensure that "current source state" is captured before any events are emitted, a full snapshot of the source is required so that the output is in a consistent "current" state before the poller starts reading and processing CDC events.
3. Doing a full snapshot of the (source) database is a very expensive and restrictive process.  When a snapshot is taking place, writes to the database must be prevented.  It is very impractical to prevent writes in a live (and high scale) system.

## Proposal

Thankfully [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b) offers a very elegant and incremental solution to the above problem.   Briefly the DBLog idea uses a low and high watermark based approach to capture the full state of a databse while interleaving events in the transaction log.   By doing so the imact on the source database is minimized.  Briefly the basic idea is:

* The reader is updated so it can be requested to "pause" and "resume" its polling of the replication slot and processing of the events.
* The reader is first paused to initiate a "snapshot" of a subset of records (this size can be chosen based on load constraints).
* When the reader is in "paused" state, it is requested to "select" a set of records.  For these records the database already has the freshest values so the intent is that the reader is to process the latest values for these records.  After this selection, the output is guaranteed to have the freshest valeus for the records selected in this set.
* When asked to "select", it first emits a "low watermark" event in the replication slot, performs the "select" of records from the source DB and then emits a "high watermark" event in the replication slot.
* The reader is then requested to resume its polling and processing (of events).

From the paper, this process looks like:

<img src="https://raw.githubusercontent.com/panyam/dbsync/main/static/images/dblog.png"/>

## DBSync

This go module is a simple implementation of DBLog.   Under the hood it does the following:

* Creates a "control namespace" in the postgresql db being synced.
* In this control namespace, it creates a Watermark table which will be used to induce low/high water mark events in our replication slot.
* Looks for an already created publication for the tables that the user is interested in tracking - this MUST be done by the user/owner of the database.  For example if the user is interested in tracking tables `users`, `restaurants`, `friends`, then a publication must for these tables must be created with the command (where `our_pub_name` is the name of the publication dbsync will be tracking):

```sql
CREATE PUBLICATION <our_pub_name> FOR TABLE users, restaurants, friends ;
```

* Takes the above publication (`our_pub_name`) and adds the watermark table to it.  This ensures that changes to the watermark table **along with** the changes to the `users`, `restaurants` and `friends` tables are all serialized to this publication in the order in which the writes were made (to the respective tables).
* Once the publication is amended, a replication slot is created (if it does not exist).  This replication slot will be ready by the reader/poller going forward.

## Getting Started

Before writing/starting your reader, we want to make sure the following are set:

### Ensure postgres

Make sure you have postgres running and that its wal_level is set to "logical" (in the postgresql.conf file)

### Setup environment variables

The following environment variables (corresponding to your database instance) are set:
  - POSTGRES_DB - Name of the Postgres DB to setup replication on
  - POSTGRES_HOST - Host where the DB is executing
  - POSTGRES_PORT - Port on which the DB is served from
  - POSTGRES_USER - Admin username to connect to the postgres db to setup replication on
  - POSTGRES_PASSWORD - Password of the admin user
  - DEFAULT_DBSYNC_CTRL_NAMESPACE - Sets the name of the control namespace where the watermark table is created.  Defaults to `dbsync_ctrl`.
  - DEFAULT_DBSYNC_WM_TABLENAME - Sets the name of the watermark table which will induce low/high watermkark events in our replication slot.  Defaults to `dbsync_wmtable`.
  - DEFAULT_DBSYNC_PUBNAME - Name of the publication that will be altered to add the watermark table to.  This MUST be created by the user/admin of the database for the tables they are interested in syncing.  Defaults to `dbsync_mypub`.
  - DEFAULT_DBSYNC_REPLSLOT - Name of the replication slot that will be created (over the publication).  Defaults to `dbsync_replslot`.

## Examples

### Simple change logger

In our basic example we simply log changes to the stdout:

```go
func main() {
  // Assuming you have created the publication over the tables to be tracked
	d, err := dbsync.NewDBSync()
	if err != nil {
		panic(err)
	}
	d.MessageHandler = &dbsync.EchoHandler{}
  d.Start()
}
```

Start your postgres db (with defaults), and as you update tables (selected for event publication) you will see the events printed on stdout.

### Custom message processing based on event type

Instead of printing every message, we can treat each message type in a custom manner.   The `DefaultMessageHandler` type allows custom function pointers that can be overridden for each message type.  If we want to handle only selected message types (eg Inserted, Deleted, Updated events) we can specify just these functions to the DefaultMessageHandler:

```go
func main() {
  // Assuming you have created the publication over the tables to be tracked
	d, err := dbsync.NewDBSync()
	if err != nil {
		panic(err)
	}
	d.MessageHandler = &dbsync.DefaultMessageHandler{
		HandleInsertMessage: func(m *dbsync.DefaultMessageHandler, idx int, msg *pglogrepl.InsertMessage, reln *pglogrepl.RelationMessage) error {
			log.Println("Handling Insert Message: ", idx, msg, reln)
			return nil
		},
		HandleDeleteMessage: func(m *dbsync.DefaultMessageHandler, idx int, msg *pglogrepl.DeleteMessage, reln *pglogrepl.RelationMessage) error {
			log.Println("Handling Delete Message: ", idx, msg, reln)
			return nil
		},
		HandleUpdateMessage: func(m *dbsync.DefaultMessageHandler, idx int, msg *pglogrepl.UpdateMessage, reln *pglogrepl.RelationMessage) error {
			log.Println("Handling Updated Message: ", idx, msg, reln)
			return nil
    },
  }
  d.Start()
}
```

### Batch processing messages with event compaction

### Inducing partial snapshots

TODO
