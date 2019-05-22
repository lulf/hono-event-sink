/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	// "context"
	"flag"
	"fmt"
	"github.com/lulf/teig-event-sink/pkg/datastore"
	"github.com/lulf/teig-event-sink/pkg/eventsource"
	"log"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"time"
)

func main() {

	var dbfile string
	var maxlogsize int
	var eventsourceAddr string
	//var username string
	//var password string
	//var usessl bool
	//var cafile string

	flag.StringVar(&dbfile, "d", "sink.db", "Path to database file")
	flag.IntVar(&maxlogsize, "m", 100, "Max number of entries in log")
	flag.StringVar(&eventsourceAddr, "e", "example.com:5672", "Address of AMQP event source")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    -e example.com:5672 [-m 100] [-d sink.db] [-u user] [-p password] [-s] [-c cafile]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	datastore, err := datastore.NewSqliteDatastore(dbfile, maxlogsize)
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer datastore.Close()

	err = datastore.Initialize()
	if err != nil {
		log.Fatal("Initializing Datastore:", err)
	}

	es := eventsource.NewAmqpEventSource(eventsourceAddr, "consumer", "foobar", true, nil)
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
