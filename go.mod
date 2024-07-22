module github.com/panyam/dbsync

go 1.22

toolchain go1.22.4

replace github.com/panyam/goutils v0.1.1 => ../../goutils/

require github.com/jackc/pglogrepl v0.0.0-20240307033717-828fbfe908e9

require (
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.5.4 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
