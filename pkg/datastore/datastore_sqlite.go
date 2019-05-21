/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

func (ds SqlDatastore) Close() {
	ds.handle.Close()
}

func NewSqliteDatastore(fileName string) (*SqlDatastore, error) {

	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		log.Print("Opening Database:", err)
		return nil, err
	}

	return &SqlDatastore{
		handle: db,
	}, nil
}

func (ds SqlDatastore) Initialize() error {
	// Create initial database table
	tableCreate := `
        create table if not exists devices (id integer not null primary key, device_id text);
	create table if not exists events (id integer not null primary key, insertion_time integer, creation_time integer, device_id integer, payload text);
	create table if not exists telemetry (id integer not null primary key, insertion_time integer, creation_time integer, device_id integer, payload text);
        `

	_, err := ds.handle.Exec(tableCreate)
	if err != nil {
		log.Print("Creating Database Tables:", err)
		return err
	}
	return nil
}

func (ds SqlDatastore) InsertNewEntry(insertTime int64, creationTime int64, deviceId string, payload string) error {
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}
	removeStmt, err := tx.Prepare("DELETE FROM telemetry WHERE ROWID IN (SELECT ROWID FROM telemetry WHERE (SELECT SUM(size) FROM telemetry AS _ WHERE insertion_time <= telemetry.insertion_time AND device_id = ?) <= 100)")
	if err != nil {
		log.Print("Preparing remove statement:", err)
		return err
	}
	defer removeStmt.Close()

	insertStmt, err := tx.Prepare("INSERT INTO telemetry(insertion_time, creation_time, device_id, payload) values(?, ?, ?, ?)")
	if err != nil {
		log.Print("Preparing insert statement:", err)
		return err
	}
	defer insertStmt.Close()

	_, err = removeStmt.Exec(deviceId)
	if err != nil {
		log.Print("Removing oldest entry:", err)
		return err
	}
	_, err = insertStmt.Exec(insertTime, creationTime, deviceId, payload)
	if err != nil {
		log.Print("Inserting entry:", err)
		return err
	}
	tx.Commit()
	return nil
}
