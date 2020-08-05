package cloud.teig.event.sink.hono;

import cloud.teig.event.sink.model.Event;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class HonoConsumer {
    @Inject
    @Channel("eventstream")
    Emitter<Event> eventEmitter;

    @Incoming("telemetry")
    public CompletionStage<Void> processTelemetry(AmqpMessage<Map> message) {
        return handleMessage(message);
    }

    @Incoming("event")
    public CompletionStage<Void> processEvent(AmqpMessage<Map> message) {
        return handleMessage(message);
    }

    private CompletionStage<Void> handleMessage(AmqpMessage<Map> message) {
        String deviceId = message.getApplicationProperties().getString("device_id");
        long creationTime = message.getCreationTime();
        Event event = new Event(deviceId, creationTime, message.getPayload());
        System.out.println("Produced event: " + event);
        return eventEmitter.send(event).thenAccept(f -> message.ack()).exceptionally(f -> {
            message.nack(f);
            return null;
        });
    }
}
