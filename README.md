# teig-iot-service

Teig IoT Service is composed of 3 components: event-sink, event-api and control-api.

The event-sink is a component that subscribes to Bosch IoT Hub telemetry and stores them in a
local database.

The event-api is an AMQP service for subscribing to the stored events for replay and receiving
notifications.

The control-api is an AMQP service for sending control messages to devices connected to Bosch IoT Hub.
