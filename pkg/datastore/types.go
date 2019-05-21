package datastore

import (
	"database/sql"
)

type Datastore interface {
	Initialize()
	InsertNewEntry(insertTime int64, creationTime int64, deviceId string, payload string) error
	Close()
}

type SqlDatastore struct {
	handle *sql.DB
}
