package cloud.teig.event.sink.hono;

import cloud.teig.event.sink.model.Event;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class HonoConsumer {
    @Inject
    @Channel("eventstream")
    Emitter<Event> eventEmitter;

    @Incoming("telemetry")
    public void processTelemetry(AmqpMessage<Map> message) {
        handleMessage(message);
    }

    @Incoming("event")
    public void processEvent(AmqpMessage<Map> message) {
        handleMessage(message);
    }

    private void handleMessage(AmqpMessage<Map> message) {
        String deviceId = message.getApplicationProperties().getString("device_id");
        long creationTime = message.getCreationTime();
        Event event = new Event(deviceId, creationTime, message.getPayload());
        System.out.println("Produced event: " + event);
        eventEmitter.send(event).thenApply(f -> message.ack())
                .exceptionally(message::nack);
    }
}
