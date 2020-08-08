package cloud.teig.event.sink.kafka;

import cloud.teig.event.sink.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class EventSerializer implements Serializer<Event> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Event event) {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("deviceId", event.getDeviceId());
            data.put("creationTime", event.getCreationTime());
            data.put("payload", event.getPayload());
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
