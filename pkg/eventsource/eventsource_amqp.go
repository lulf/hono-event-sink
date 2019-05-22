/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventsource

import (
	"crypto/tls"
	"crypto/x509"
	"net"
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
	tlsEnabled    bool
	caPem         []byte
	tcpConn       net.Conn
	amqpConn      electron.Connection
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

	if es.amqpConn == nil {
		opts := []electron.ConnectionOption{
			electron.ContainerId("event-sink"),
		}
		if es.username != "" && es.password != "" {
			opts = append(opts, electron.SASLEnable(), electron.SASLAllowInsecure(true), electron.User(es.username), electron.Password([]byte(es.password)))
		}
		amqpConn, err := electron.NewConnection(es.tcpConn, opts...)
		if err != nil {
			return nil, err
		}
		es.amqpConn = amqpConn
	}

	ropts := []electron.LinkOption{electron.Source(source)}
	r, err := es.amqpConn.Receiver(ropts...)
	if err != nil {
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
