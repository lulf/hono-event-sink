package cloud.teig.event.sink.model;

import java.util.Map;

public class Event {
    private final String deviceId;
    private final long creationTime;
    private final Map payload;

    public Event(String deviceId, long creationTime, Map payload) {
        this.deviceId = deviceId;
        this.creationTime = creationTime;
        this.payload = payload;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Map getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Event{" +
                "deviceId='" + deviceId + '\'' +
                ", creationTime=" + creationTime +
                ", payload=" + payload +
                '}';
    }
}
