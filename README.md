# hono-event-sink

The event-sink is a component that subscribes to Hono telemetry and event addresses and stores them in Kafka

The following environment variables can be set to influence the process:

```
KAFKA_BOOTSTRAP_SERVERS
MP_MESSAGING_OUTGOING_EVENTSTREAM_ADDRESS
MP_MESSAGING_INCOMING_TELEMETRY_ADDRESS
MP_MESSAGING_INCOMING_EVENT_ADDRESS
AMQP_HOST
AMQP_PORT
```
