/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventsource

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"testing"
)

type TestServer struct {
	l net.Listener
	s string
	e error
	c chan amqp.Message
}

func NewTestServer(t *testing.T, source string) *TestServer {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	return &TestServer{
		l: l,
		s: source,
		e: nil,
		c: make(chan amqp.Message),
	}
}

func (s *TestServer) close() {
	s.l.Close()
	close(s.c)
}

func (s *TestServer) run(t *testing.T) {
	cont := electron.NewContainer("test-server")
	c, err := cont.Accept(s.l)
	if err != nil {
		t.Fatal(err)
	}
	s.l.Close() // This server only accepts one connection

	var snd electron.Sender
	for snd == nil {
		in := <-c.Incoming()
		switch in := in.(type) {
		case *electron.IncomingSession, *electron.IncomingConnection:
			in.Accept() // Accept the incoming connection and session for the sender
		case *electron.IncomingSender:

			if s.s != in.Source() {
				s.e = fmt.Errorf("Sender source '%s' does not match expected source '%s'", in.Source(), s.s)
			}
			snd = in.Accept().(electron.Sender)
		case nil:
			return // Connection is closed
		default:
			in.Reject(amqp.Errorf("test-server", "unexpected endpoint %v", in))
		}
	}
	go func() { // Reject any further incoming endpoints
		for in := range c.Incoming() {
			in.Reject(amqp.Errorf("test-server", "unexpected endpoint %v", in))
		}
	}()
	// Send messaging from channel until closed
	for {
		m, more := <-s.c
		if more {
			snd.SendSync(m)
		} else {
			return
		}
	}

}

func TestSubscribe(t *testing.T) {
	server := NewTestServer(t, "data")
	defer server.close()
	go server.run(t)

	es := NewAmqpEventSource(server.l.Addr().String(), "", "", false, nil)

	s, err := es.Subscribe("data")
	assert.NotNil(t, s)
	assert.Nil(t, err)

	tm := amqp.NewMessage()
	tm.ApplicationProperties()["device_id"] = "dev1"
	tm.Marshal("test")

	server.c <- tm

	rm, err := s.Receive()

	m := rm.Message
	assert.Nil(t, server.e)
	assert.Equal(t, tm, m)
}
