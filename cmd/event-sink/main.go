/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"encoding/json"
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
	var tenantId string
	var password string
	var tlsEnabled bool
	var cafile string

	flag.StringVar(&eventstoreAddr, "a", "amqp://127.0.0.1:5672", "Address of AMQP event store")
	flag.StringVar(&eventsourceAddr, "e", "messaging.bosch-iot-hub.com:5671", "Address of AMQP event source")
	flag.StringVar(&tenantId, "t", "", "Tenant ID for Bosch IoT Hub")
	flag.StringVar(&password, "p", "", "Password for AMQP event source")
	flag.BoolVar(&tlsEnabled, "s", false, "Enable TLS")
	flag.StringVar(&cafile, "c", "", "Certificate CA file")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    -e example.com:5672 [-t tenant_id] [-p password] [-s] [-c cafile] [-a 127.0.0.1:5672] \n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if eventsourceAddr == "" {
		log.Fatal("Missing address of AMQP event source")
	}

	eventStore := eventstore.NewAmqpEventStore(eventstoreAddr)

	telemetryPub, err := eventStore.Publisher("events")
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

	username := fmt.Sprintf("messaging@%s", tenantId)
	es := eventsource.NewAmqpEventSource(eventsourceAddr, username, password, tlsEnabled, ca)
	defer es.Close()

	telemetrySub, err := es.Subscribe(fmt.Sprintf("telemetry/%s", tenantId))
	if err != nil {
		log.Fatal("Subscribing to telemetry:", err)
	}

	eventSub, err := es.Subscribe(fmt.Sprintf("event/%s", tenantId))
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

	var result map[string]interface{} = make(map[string]interface{}, 0)
	err := json.Unmarshal(message.GetData(), &result)
	if err != nil {
		return err
	}
	event := eventstore.NewEvent(deviceId, creationTime, result)

	return pub.Send(event)
}
