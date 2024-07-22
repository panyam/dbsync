
# DBSync

A simple change data capture (CDC) processing library for postgres based on [DBLog](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b).

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

### Create a database

The default database on postgres is `postgres`.  We will use this for our example.

POSTGRES_NAME=postgres

### Create source tables

We will create a couple of source tables which we will continually sync and track their changes on.  In our example we have 3 tables:

* Users table

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
