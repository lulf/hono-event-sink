/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	// "context"
	"crypto/tls"
	"crypto/x509"
	"github.com/lulf/teig-event-sink/pkg/datastore"
	"log"
	"time"
	// "qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {

	dbfile := "./test.db"

	datastore, err := datastore.NewSqliteDatastore(dbfile)
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer datastore.Close()

	err = datastore.Initialize()
	if err != nil {
		log.Fatal("Initializing Datastore:", err)
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

			deviceId := message.ApplicationProperties()["device_id"].(string)
			creationTime := message.Properties()["creation-time"].(int64)
			payload := message.Body().(string)

			err := datastore.InsertNewEntry(insertTime, creationTime, deviceId, payload)
			if err != nil {
				log.Fatal("Insert entry into datastore:", err)
			}
			rm.Accept()
		} else if err == electron.Closed {
			return
		} else {
			log.Fatal("Receive message:", err)
		}
	}

}
