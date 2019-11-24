/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventstore

import (
	"context"
	"encoding/json"
	"log"

	"pack.ag/amqp"
)

type AmqpPublisher struct {
	snd *amqp.Sender
}

type AmqpEventStore struct {
	address    string
	client     *amqp.Client
	publishers []*AmqpPublisher
}

func NewAmqpEventStore(address string) *AmqpEventStore {
	return &AmqpEventStore{
		address:    address,
		client:     nil,
		publishers: make([]*AmqpPublisher, 0),
	}
}

func (es *AmqpEventStore) Close() error {
	for _, pub := range es.publishers {
		pub.Close()
	}

	if es.client != nil {
		es.client.Close()
	}

	return nil
}

func (es *AmqpEventStore) Publisher(target string) (*AmqpPublisher, error) {
	if es.client == nil {
		client, err := amqp.Dial(es.address)
		if err != nil {
			return nil, err
		}
		es.client = client
	}

	session, err := es.client.NewSession()
	if err != nil {
		return nil, err
	}

	s, err := session.NewSender(amqp.LinkTargetAddress(target))
	if err != nil {
		return nil, err
	}

	pub := &AmqpPublisher{
		snd: s,
	}
	es.publishers = append(es.publishers, pub)
	return pub, nil
}

func (pub *AmqpPublisher) Send(event *Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		log.Print("Serializing event:", err)
		return err
	}
	m := amqp.NewMessage(data)
	err = pub.snd.Send(context.TODO(), m)
	if err != nil {
		return err
	}
	return nil

}

func (pub *AmqpPublisher) Close() {
	pub.snd.Close(nil)
}

func NewEvent(deviceId string, creationTime int64, data map[string]interface{}) *Event {
	return &Event{
		CreationTime: creationTime,
		DeviceId:     deviceId,
		Data:         data,
	}
}
