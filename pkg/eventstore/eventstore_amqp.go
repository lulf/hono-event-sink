/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventstore

import (
	"encoding/json"
	"log"
	"net"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type AmqpPublisher struct {
	snd electron.Sender
}

type AmqpEventStore struct {
	address    string
	tcpConn    net.Conn
	amqpConn   electron.Connection
	publishers []*AmqpPublisher
}

func NewAmqpEventStore(address string) *AmqpEventStore {
	return &AmqpEventStore{
		address:    address,
		tcpConn:    nil,
		amqpConn:   nil,
		publishers: make([]*AmqpPublisher, 0),
	}
}

func (es *AmqpEventStore) Close() error {
	for _, pub := range es.publishers {
		pub.Close()
	}

	if es.amqpConn != nil {
		es.amqpConn.Close(nil)
	}

	if es.tcpConn != nil {
		es.tcpConn.Close()
	}

	return nil
}

func (es *AmqpEventStore) Publisher(target string) (*AmqpPublisher, error) {
	if es.tcpConn == nil {
		tcpConn, err := net.Dial("tcp", es.address)
		if err != nil {
			return nil, err
		}
		es.tcpConn = tcpConn
	}

	if es.amqpConn == nil {
		opts := []electron.ConnectionOption{
			electron.ContainerId("event-sink"),
		}
		amqpConn, err := electron.NewConnection(es.tcpConn, opts...)
		if err != nil {
			return nil, err
		}
		es.amqpConn = amqpConn
	}

	sopts := []electron.LinkOption{electron.Target(target)}
	s, err := es.amqpConn.Sender(sopts...)
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
	m := amqp.NewMessage()
	data, err := json.Marshal(event)
	if err != nil {
		log.Print("Serializing event:", err)
		return err
	}
	m.Marshal(data)
	outcome := pub.snd.SendSync(m)
	if outcome.Status == electron.Unsent || outcome.Status == electron.Unacknowledged {
		return outcome.Error
	}
	return nil

}

func (pub *AmqpPublisher) Close() {
	pub.snd.Close(nil)
}
