/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventsource

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type MessageHandler func(message amqp.Message) error

type AmqpSubscription struct {
	rcv electron.Receiver
}

type AmqpEventSource struct {
	address       string
	username      string
	password      string
	caPem         []byte
	tlsConn       *tls.Conn
	amqpConn      electron.Connection
	subscriptions []*AmqpSubscription
}

func NewAmqpEventSource(address string, username string, password string, caPem []byte) *AmqpEventSource {
	return &AmqpEventSource{
		address:       address,
		username:      username,
		password:      password,
		caPem:         caPem,
		tlsConn:       nil,
		amqpConn:      nil,
		subscriptions: make([]*AmqpSubscription, 0),
	}
}

func (es *AmqpEventSource) Close() error {
	for _, sub := range es.subscriptions {
		sub.Close()
	}

	if es.amqpConn != nil {
		es.amqpConn.Close(nil)
	}

	if es.tlsConn != nil {
		es.tlsConn.Close()
	}

	return nil
}

func (es *AmqpEventSource) Subscribe(source string) (*AmqpSubscription, error) {
	if es.tlsConn == nil {
		certPool := x509.NewCertPool()
		if es.caPem != nil {
			certPool.AppendCertsFromPEM(es.caPem)
		}
		config := tls.Config{RootCAs: certPool}
		tlsConn, err := tls.Dial("tcp", es.address, &config)
		if err != nil {
			log.Print("Dialing TLS endpoint:", err)
			return nil, err
		}
		es.tlsConn = tlsConn
	}

	if es.amqpConn == nil {
		opts := []electron.ConnectionOption{
			electron.ContainerId("event-sink"),
			electron.SASLEnable(),
			electron.SASLAllowInsecure(true),
			electron.User(es.username),
			electron.Password([]byte(es.password)),
		}
		amqpConn, err := electron.NewConnection(es.tlsConn, opts...)
		if err != nil {
			log.Print("Connecting AMQP server:", err)
			return nil, err
		}
		es.amqpConn = amqpConn
	}

	ropts := []electron.LinkOption{electron.Source(source)}
	r, err := es.amqpConn.Receiver(ropts...)
	if err != nil {
		log.Print("Creating receiver:", err)
		return nil, err
	}

	sub := &AmqpSubscription{
		rcv: r,
	}
	es.subscriptions = append(es.subscriptions, sub)
	return sub, nil
}

func (sub *AmqpSubscription) Receive() (electron.ReceivedMessage, error) {
	return sub.rcv.Receive()
}

func (sub *AmqpSubscription) Close() {
	sub.rcv.Close(nil)
}
