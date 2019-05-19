package main

import (
	// "context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
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
			// TODO: Store in sqlite3 db
			rm.Accept()
		} else if err == electron.Closed {
			return
		} else {
			log.Fatal("Receive message:", err)
		}
	}

}
