package main

import (
	// "context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
	// "qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {

	dbfile := "./test.db"

	db, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		log.Fatal("Opening Database:", err)
	}
	defer db.Close()

	// Create initial database table
	tableCreate := `
        create table if not exists devices (id integer not null primary key, device_id text);
	create table if not exists events (id integer not null primary key, insertion_time integer, creation_time integer, device_id integer, payload text);
	create table if not exists telemetry (id integer not null primary key, insertion_time integer, creation_time integer, device_id integer, payload text);
        `

	_, err = db.Exec(tableCreate)
	if err != nil {
		log.Fatal("Creating Database Tables:", err)
	}

	// create a pool of trusted certs
	certPool := x509.NewCertPool()
	// TODO: certPool.AppendCertsFromPEM(messagingCaPEM)

	// configure a client to use trust those certificates

	config := tls.Config{RootCAs: certPool, InsecureSkipVerify: true}
	tlsConn, err := tls.Dial("tcp", "messaging-h8gi9otom6-enmasse-infra.192.168.1.56.nip.io:443", &config)
	if err != nil {
		log.Fatal("Dialing TLS endpoint:", err)
	}
	defer tlsConn.Close()

	opts := []electron.ConnectionOption{
		electron.ContainerId("event-sink"),
		electron.SASLEnable(),
		electron.SASLAllowInsecure(true),
		electron.User("consumer"),
		electron.Password([]byte("foobar")),
	}
	conn, err := electron.NewConnection(tlsConn, opts...)
	if err != nil {
		log.Fatal("Connecting AMQP server:", err)
	}
	defer conn.Close(nil)

	ropts := []electron.LinkOption{electron.Source("telemetry/teig.iot")}
	receiver, err := conn.Receiver(ropts...)
	for {
		if rm, err := receiver.Receive(); err == nil {
			message := rm.Message

			insertTime := time.Now().UTC().Unix()

			deviceId := message.ApplicationProperties()["device_id"]
			creationTime := message.Properties()["creation-time"]
			payload := message.Body()

			tx, err := db.Begin()
			if err != nil {
				log.Fatal("Starting transaction:", err)
			}
			removeStmt, err := tx.Prepare("DELETE FROM telemetry WHERE ROWID IN (SELECT ROWID FROM telemetry WHERE (SELECT SUM(size) FROM telemetry AS _ WHERE insertion_time <= telemetry.insertion_time AND device_id = ?) <= 100)")
			if err != nil {
				log.Fatal("Preparing remove statement:", err)
			}
			defer removeStmt.Close()

			insertStmt, err := tx.Prepare("INSERT INTO telemetry(insertion_time, creation_time, device_id, payload) values(?, ?, ?, ?)")
			if err != nil {
				log.Fatal("Preparing insert statement:", err)
			}
			defer insertStmt.Close()

			_, err = removeStmt.Exec(deviceId)
			if err != nil {
				log.Fatal("Removing oldest entry:", err)
			}
			_, err = insertStmt.Exec(insertTime, creationTime, deviceId, payload)
			if err != nil {
				log.Fatal("Inserting entry:", err)
			}
			tx.Commit()
			rm.Accept()
		} else if err == electron.Closed {
			return
		} else {
			log.Fatal("Receive message:", err)
		}
	}

}
