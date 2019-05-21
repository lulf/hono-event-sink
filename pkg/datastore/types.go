/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
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
	handle  *sql.DB
	maxSize int
}
