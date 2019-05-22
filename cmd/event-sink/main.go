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
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"time"
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

	eventSub, err := es.Subscribe("event/teig.iot")
	if err != nil {
		log.Fatal("Subscribing to event:", err)
	}

	go func() {
		runSink(datastore, telemetrySub)
	}()

	go func() {
		runSink(datastore, eventSub)
	}()
}

func runSink(datastore datastore.Datastore, sub *eventsource.AmqpSubscription) {
	for {
		if rm, err := sub.Receive(); err == nil {
			err := handleMessage(datastore, rm.Message)
			if err != nil {
				log.Print("Insert entry into datastore:", err)
				rm.Reject()
			} else {
				rm.Accept()
			}
		} else if err == electron.Closed {
			log.Print("Telemetry subscription closed")
			return
		} else {
			log.Print("Receive telemetry message:", err)
		}
	}
}

func handleMessage(datastore datastore.Datastore, message amqp.Message) error {
	insertTime := time.Now().UTC().Unix()

	deviceId := message.ApplicationProperties()["device_id"].(string)
	creationTime := message.Properties()["creation-time"].(int64)
	payload := message.Body().(string)

	return datastore.InsertNewEntry(insertTime, creationTime, deviceId, payload)
}
