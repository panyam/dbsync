package dbsync

import (
	"fmt"
	"log"
	"strconv"

	"github.com/jackc/pglogrepl"
	gut "github.com/panyam/goutils/utils"
)

func ConnStr(DBName string, Host string, Port int, Username string, Password string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Host, Port, Username, Password, DBName)
}

// Gets the postgres connection string from environment variables.
func PGConnStringFromEnv() string {
	out := gut.GetEnvOrDefault("POSTGRES_CONNSTR", "", false)
	if out == "" {
		dbname := gut.GetEnvOrDefault("POSTGRES_DB", DEFAULT_POSTGRES_DB)
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

func MessageToMap(p *DBSync, msg *pglogrepl.TupleData, reln *pglogrepl.RelationMessage) (pkey string, out map[string]any, errors map[string]error) {
	msgcols := msg.Columns
	relcols := reln.Columns
	if len(msgcols) != len(relcols) {
		log.Printf("Msg cols (%d) and Rel cols (%d) dont match", len(msgcols), len(relcols))
	}
	// fullschema := fmt.Sprintf("%s.%s", reln.Namespace, reln.RelationName)
	// log.Printf("Namespace: %s, RelName: %s, FullSchema: %s", reln.Namespace, reln.RelationName, fullschema)
	pkey = "id"
	if out == nil {
		out = make(map[string]any)
	}
	tableinfo := p.GetTableInfo(reln.RelationID)
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
