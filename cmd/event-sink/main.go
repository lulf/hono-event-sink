/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	// "context"
	"github.com/lulf/teig-event-sink/pkg/datastore"
	"github.com/lulf/teig-event-sink/pkg/eventsource"
	"log"
	"time"
	// "qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {

	dbfile := "./test.db"

	datastore, err := datastore.NewSqliteDatastore(dbfile, 10)
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer datastore.Close()

	err = datastore.Initialize()
	if err != nil {
		log.Fatal("Initializing Datastore:", err)
	}

	es := eventsource.NewAmqpEventSource("messaging-h8gi9otom6-enmasse-infra.192.168.1.56.nip.io:443", "consumer", "foobar", nil)
	defer es.Close()

	telemetrySub, err := es.Subscribe("telemetry/teig.iot")
	if err != nil {
		log.Fatal("Subscribing to telemetry:", err)
	}
	for {
		if rm, err := telemetrySub.Receive(); err == nil {
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
			log.Print("Subscription closed")
			return
		} else {
			log.Fatal("Receive message:", err)
		}
	}

}
