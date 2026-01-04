package local;

import com.google.cloud.Timestamp;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SpannerToPubsubTest {
    @Test
    void computeStartTimestampSubtractsOffsetAndOneSecond() {
        Timestamp now = Timestamp.ofTimeSecondsAndNanos(100, 0);
        Timestamp start = SpannerToPubsub.computeStartTimestamp(now, 60);
        assertEquals(39, start.getSeconds());
        assertEquals(0, start.getNanos());
    }

    @Test
    void computeStartTimestampFloorsAtZero() {
        Timestamp now = Timestamp.ofTimeSecondsAndNanos(30, 0);
        Timestamp start = SpannerToPubsub.computeStartTimestamp(now, 60);
        assertEquals(0, start.getSeconds());
    }

    @Test
    void extractPayloadFromJsonBase64Decodes() {
        byte[] payload = SpannerToPubsub.extractPayloadFromJson("{\"Payload\":\"aGVsbG8=\"}");
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), payload);
    }

    @Test
    void extractPayloadFromJsonFallsBackToRawString() {
        byte[] payload = SpannerToPubsub.extractPayloadFromJson("{\"Payload\":\"hello!\"}");
        assertArrayEquals("hello!".getBytes(StandardCharsets.UTF_8), payload);
    }

    @Test
    void extractPayloadFromJsonIsCaseInsensitive() {
        byte[] payload = SpannerToPubsub.extractPayloadFromJson("{\"payload\":\"aGVsbG8=\"}");
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), payload);
    }

    @Test
    void extractPayloadFromJsonReturnsNullWhenMissing() {
        assertNull(SpannerToPubsub.extractPayloadFromJson("{\"Other\":\"x\"}"));
        assertNull(SpannerToPubsub.extractPayloadFromJson(""));
        assertNull(SpannerToPubsub.extractPayloadFromJson(null));
    }
}
