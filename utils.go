package dbsync

import (
	"fmt"
	"strconv"

	gut "github.com/panyam/goutils/utils"
)

func ConnStr(DBName string, Host string, Port int, Username string, Password string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, Username, Password, DBName)
}

// Gets the postgres connection string from environment variables.
func PGConnStringFromEnv() string {
	out := gut.GetEnvOrDefault("POSTGRES_CONNSTR", "", false)
	if out == "" {
		dbname := gut.GetEnvOrDefault("POSTGRES_DB", DEFAULT_POSTGRES_DB, true)
		dbhost := gut.GetEnvOrDefault("POSTGRES_HOST", DEFAULT_POSTGRES_HOST, true)
		dbuser := gut.GetEnvOrDefault("POSTGRES_USER", DEFAULT_POSTGRES_USER, true)
		dbpassword := gut.GetEnvOrDefault("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD, true)
		dbport := gut.GetEnvOrDefault("POSTGRES_PORT", DEFAULT_POSTGRES_PORT, true)
		portval, err := strconv.Atoi(dbport)
		if err != nil {
			panic(err)
		}
		out = ConnStr(dbname, dbhost, portval, dbuser, dbpassword)
	}
	return out
}
