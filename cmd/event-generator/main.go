/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"pack.ag/amqp"

	"github.com/lulf/teig-event-sink/pkg/eventstore"
)

func main() {
	var numdevices int
	flag.IntVar(&numdevices, "n", 5, "Number of devices to simulate")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-n 5]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	client, err := amqp.Dial("amqp://127.0.0.1:5672")
	if err != nil {
		log.Fatal("Dial:", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Session:", err)
	}
	for id := 1; id <= numdevices; id++ {
		device := fmt.Sprintf("Dings %d", id)
		s, err := session.NewSender(amqp.LinkTargetAddress("events"))
		if err != nil {
			log.Fatal("Sender:", s)
		}
		wait := 2 + rand.Intn(5)
		go runSender(device, s, wait)
	}

	for {
		time.Sleep(time.Duration(6000))
	}
}

func runSender(device string, sender *amqp.Sender, wait int) {
	log.Print(fmt.Sprintf("Running %s with interval %d", device, wait))
	step := 0.1
	startValue := rand.Float64() * math.Pi
	value := startValue
	maxValue := 2.0 * math.Pi
	for {
		time.Sleep(time.Duration(wait) * time.Second)
		now := time.Now().UTC().Unix()
		if value > maxValue {
			value = 0.0
		}

		payload := make(map[string]interface{})
		payload["temperature"] = 15.0 + (10.0 * math.Sin(value))
		value += step
		event := eventstore.NewEvent(device, now, payload)
		data, err := json.Marshal(event)
		if err != nil {
			log.Print("Serializing event:", device, err)
			continue
		}
		log.Print(fmt.Sprintf("Woke up %s to send data: %s", device, data))
		m := amqp.NewMessage(data)
		err = sender.Send(context.TODO(), m)
		if err != nil {
			log.Print("Sending event:", device, err)
			continue
		}
	}
}
