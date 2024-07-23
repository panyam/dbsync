
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

<img src="https://buildmage.com/static/images/part9/current-search-architecture.svg" />

* The reader is updated so it can be requested to "pause" and "resume" its polling of the replication slot and processing of the events.
* The reader is first paused to initiate a "snapshot" of a subset of records (this size can be chosen based on load constraints).
* When the reader is in "paused" state, it is requested to "select" a set of records.  For these records the database already has the freshest values so the intent is that the reader is to process the latest values for these records.  After this selection, the output is guaranteed to have the freshest valeus for the records selected in this set.
* When asked to "select", it first emits a "low watermark" event in the replication slot, performs the "select" of records from the source DB and then emits a "high watermark" event in the replication slot.
* The reader is then requested to resume its polling and processing (of events).

From the paper, this process looks like:

## Basic Example

### Prerequisites

* You have postgres running.
* Make sure your postgres instance's wal_level is set to "logical" (in the postgresql.conf file)
* The following environment variables (corresponding to your database instance) are set:
  - POSTGRES_NAME - Name of the Postgres DB to setup replication on
  - POSTGRES_HOST - Host where the DB is executing
  - POSTGRES_PORT - Port on which the DB is served from
  - POSTGRES_USER - Admin username to connect to the postgres db to setup replication on
  - POSTGRES_PASSWORD - Password of the admin user

### Start postgres

The default database on postgres is `postgres`.  We will use this for our example.

POSTGRES_NAME=postgres

### Create source tables

We will create a couple of source tables which we will continually sync and track their changes on.  In our example we have 3 tables:

#### Users table

This captures information about a user in our system.

```sql
CREATE TABLE Users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    avatar VARCHAR(255),
    homepage VARCHAR(255),
    date_of_birth DATE,
    address TEXT,
    interests TEXT[]
);
```
 
#### Restaurant table

This captures information about a restaurant that our users will visit and leave ratings on

```sql
CREATE TABLE Restaurant (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    address TEXT,
    rating INT,
    tags TEXT[]
);
```

#### Visit table

This captures information about a user's particular visit to a restaurant.

```sql
CREATE TABLE Visit (
    user_id INT REFERENCES Users(id),
    restaurant_id VARCHAR(255) REFERENCES Restaurant(id),
    amount_spent DECIMAL(10, 2) NOT NULL,
    visit_rating INT,
    PRIMARY KEY (user_id, restaurant_id)
);
```

### Setup our synd pipeline

Open example.go and start the main method:



References
* dblog paper from netflix
* Dolt - https://www.dolthub.com/blog/2024-03-08-postgres-logical-replication/
