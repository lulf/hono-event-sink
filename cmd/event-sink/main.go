/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"pack.ag/amqp"

	"github.com/lulf/teig-event-sink/pkg/eventsource"
	"github.com/lulf/teig-event-sink/pkg/eventstore"
)

func main() {

	var eventstoreAddr string
	var eventsourceAddr string
	var username string
	var password string
	var tlsEnabled bool
	var cafile string

	flag.StringVar(&eventstoreAddr, "a", "amqp://127.0.0.1:5672", "Address of AMQP event store")
	flag.StringVar(&eventsourceAddr, "e", "", "Address of AMQP event source")
	flag.StringVar(&username, "u", "", "Username for AMQP event source")
	flag.StringVar(&password, "p", "", "Password for AMQP event source")
	flag.BoolVar(&tlsEnabled, "s", false, "Enable TLS")
	flag.StringVar(&cafile, "c", "", "Certificate CA file")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    -e example.com:5672 [-u user] [-p password] [-s] [-c cafile] [-a 127.0.0.1:5672] \n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if eventsourceAddr == "" {
		log.Fatal("Missing address of AMQP event source")
	}

	eventStore := eventstore.NewAmqpEventStore(eventstoreAddr)

	telemetryPub, err := eventStore.Publisher("telemetry")
	if err != nil {
		log.Fatal("Creating publisher for telemetry:", err)
	}

	eventPub, err := eventStore.Publisher("events")
	if err != nil {
		log.Fatal("Creating publisher for events:", err)
	}

	var ca []byte
	if cafile != "" {
		ca, err = ioutil.ReadFile(cafile)
		if err != nil {
			log.Fatal("Reading CA file:", err)
		}
	}

	es := eventsource.NewAmqpEventSource(eventsourceAddr, username, password, tlsEnabled, ca)
	defer es.Close()

	telemetrySub, err := es.Subscribe("telemetry/tad3e7d23cfe04e04ba0b98859744a063")
	if err != nil {
		log.Fatal("Subscribing to telemetry:", err)
	}

	eventSub, err := es.Subscribe("event/tad3e7d23cfe04e04ba0b98859744a063")
	if err != nil {
		log.Fatal("Subscribing to event:", err)
	}

	go func() {
		runSink(telemetryPub, telemetrySub)
	}()

	runSink(eventPub, eventSub)
}

func runSink(pub *eventstore.AmqpPublisher, sub *eventsource.AmqpSubscription) {
	for {
		if msg, err := sub.Receive(); err == nil {
			err := handleMessage(pub, msg)
			if err != nil {
				log.Print("Insert entry into datastore:", err)
				msg.Reject(nil)
			} else {
				msg.Accept()
			}
		} else {
			log.Print("Receive telemetry message:", err)
		}
	}
}

func handleMessage(pub *eventstore.AmqpPublisher, message *amqp.Message) error {
	deviceId := message.ApplicationProperties["device_id"].(string)
	creationTime := message.Properties.CreationTime.Unix()
	payload := string(message.GetData())

	event := eventstore.NewEvent(deviceId, creationTime, payload)

	return pub.Send(event)
}
