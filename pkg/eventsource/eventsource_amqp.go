/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventsource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"

	"pack.ag/amqp"
)

type MessageHandler func(message amqp.Message) error

type AmqpSubscription struct {
	rcv *amqp.Receiver
}

type AmqpEventSource struct {
	address       string
	username      string
	password      string
	tlsEnabled    bool
	caPem         []byte
	tcpConn       net.Conn
	amqpClient    *amqp.Client
	subscriptions []*AmqpSubscription
}

func NewAmqpEventSource(address string, username string, password string, tlsEnabled bool, caPem []byte) *AmqpEventSource {
	return &AmqpEventSource{
		address:       address,
		username:      username,
		password:      password,
		tlsEnabled:    tlsEnabled,
		caPem:         caPem,
		tcpConn:       nil,
		amqpClient:    nil,
		subscriptions: make([]*AmqpSubscription, 0),
	}
}

func (es *AmqpEventSource) Close() error {
	for _, sub := range es.subscriptions {
		sub.Close()
	}

	if es.amqpClient != nil {
		es.amqpClient.Close()
	}

	if es.tcpConn != nil {
		es.tcpConn.Close()
	}

	return nil
}

func (es *AmqpEventSource) Subscribe(source string) (*AmqpSubscription, error) {
	if es.tcpConn == nil {
		if es.tlsEnabled {
			certPool := x509.NewCertPool()
			if es.caPem != nil {
				certPool.AppendCertsFromPEM(es.caPem)
			}
			config := tls.Config{RootCAs: certPool}
			tlsConn, err := tls.Dial("tcp", es.address, &config)
			if err != nil {
				return nil, err
			}
			es.tcpConn = tlsConn
		} else {
			tcpConn, err := net.Dial("tcp", es.address)
			if err != nil {
				return nil, err
			}
			es.tcpConn = tcpConn
		}
	}

	if es.amqpClient == nil {
		opts := []amqp.ConnOption{
			amqp.ConnContainerID("event-sink"),
		}
		if es.username != "" && es.password != "" {
			opts = append(opts, amqp.ConnSASLPlain(es.username, es.password))
		}
		amqpClient, err := amqp.New(es.tcpConn, opts...)
		if err != nil {
			return nil, err
		}
		es.amqpClient = amqpClient
	}

	session, err := es.amqpClient.NewSession()
	if err != nil {
		return nil, err
	}
	r, err := session.NewReceiver(amqp.LinkSourceAddress(source))
	if err != nil {
		return nil, err
	}

	sub := &AmqpSubscription{
		rcv: r,
	}
	es.subscriptions = append(es.subscriptions, sub)
	return sub, nil
}

func (sub *AmqpSubscription) Receive() (*amqp.Message, error) {
	return sub.rcv.Receive(context.TODO())
}

func (sub *AmqpSubscription) Close() {
	sub.rcv.Close(nil)
}
