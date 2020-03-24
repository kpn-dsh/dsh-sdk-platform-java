import com.google.protobuf.ByteString;
import dsh.messages.Envelope;
import dsh.messages.Serdes;
import dsh.sdk.kafka.serdes.DataEnvelopeDeserializer;
import dsh.sdk.kafka.serdes.DataEnvelopeSerializer;
import dsh.sdk.kafka.serdes.KeyEnvelopeDeserializer;
import dsh.sdk.kafka.serdes.KeyEnvelopeSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;
import utils.Loop;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SerdeTests {
    private static final Logger logger = Logger.getLogger(SerdeTests.class.getName());

    private final Envelope.KeyEnvelope refKey = Envelope.KeyEnvelope.newBuilder()
            .setKey("my/sample/key")
            .setHeader(
                    Envelope.KeyHeader.newBuilder()
                            .setQos(Envelope.QoS.RELIABLE)
                            .setRetained(true)
                            .setIdentifier(
                                    Envelope.Identity.newBuilder()
                                            .setApplication("my-sample-app")
                                            .setTenant("me")
                            )
            )
            .build();

    private final Envelope.DataEnvelope refValue = Envelope.DataEnvelope.newBuilder()
            .setPayload(ByteString.copyFromUtf8("ye olde connected clock shoppe"))
            .build();

    private final KeyEnvelopeSerializer key_serializer = new KeyEnvelopeSerializer();
    private final KeyEnvelopeDeserializer key_deserializer = new KeyEnvelopeDeserializer();
    private final DataEnvelopeSerializer data_serializer = new DataEnvelopeSerializer();
    private final DataEnvelopeDeserializer data_deserializer = new DataEnvelopeDeserializer();

    private final byte[] refKey_bytes = key_serializer.serialize(null, refKey);
    private final byte[] refValue_bytes = data_serializer.serialize(null, refValue);

    @Test
    public void kafkaKeyEnvelopeSerdesShouldBeIdempotent() {
        assertEquals(
                refKey,
                new KeyEnvelopeDeserializer().deserialize("dummy-topic", new KeyEnvelopeSerializer().serialize("dummy-topic", refKey))
        );
    }

    @Test
    public void kafkaKeyEnvelopeSerdesCanHandleNULL() {
        assertNull(new KeyEnvelopeDeserializer().deserialize("dummy-topic", null));
        assertNull(new KeyEnvelopeSerializer().serialize("dummy-topic", null));
    }

    @Test
    public void kafkaDataEnvelopeSerdesShouldBeIdempotent() {
        assertEquals(
                refValue,
                new DataEnvelopeDeserializer().deserialize("dummy-topic", new DataEnvelopeSerializer().serialize("dummy-topic", refValue))
        );
    }

    @Test
    public void kafkaDataEnvelopeSerdesCanHandleNULL() {
        assertNull(new DataEnvelopeDeserializer().deserialize("dummy-topic", null));
        assertNull(new DataEnvelopeSerializer().serialize("dummy-topic", null));
    }

    @Test
    @Tag("performance")
    public void keyEnvelopeDeserializationShouldBeFast(TestReporter testReporter, TestInfo testInfo) {
        long ops = new Loop(5, TimeUnit.SECONDS).run(() -> key_deserializer.deserialize(null, refKey_bytes)).ops();
        testReporter.publishEntry("key deserializing ops/s", Long.toString(ops));
    }

    @Test
    @Tag("performance")
    public void keyEnvelopeSerializationShouldBeFast(TestReporter testReporter) {
        long ops = new Loop(5, TimeUnit.SECONDS).run(() -> key_serializer.serialize(null, refKey)).ops();
        testReporter.publishEntry("key serializing ops/s", Long.toString(ops));
    }

    @Test
    @Tag("performance")
    public void dataEnvelopeDeserializationShouldBeFast(TestReporter testReporter) {
        long ops = new Loop(5, TimeUnit.SECONDS).run(() -> data_deserializer.deserialize(null, refValue_bytes)).ops();
        testReporter.publishEntry("data deserializing ops/s", Long.toString(ops));
    }

    @Test
    @Tag("performance")
    public void dataEnvelopeSerializationShouldBeFast(TestReporter testReporter) {
        long ops = new Loop(5, TimeUnit.SECONDS).run(() -> data_serializer.serialize(null, refValue)).ops();
        testReporter.publishEntry("data serializing ops/s", Long.toString(ops));
    }

    @Test
    public void dataEnvelopeDeserializingFunctionCanBeMapped() {
        List<Envelope.DataEnvelope> a = Arrays.asList(
                Envelope.DataEnvelope.newBuilder().build(),
                Envelope.DataEnvelope.newBuilder().build(),
                Envelope.DataEnvelope.newBuilder().build()
        );
        List<byte[]> la = a.stream().map(Serdes.serializeValue).collect(Collectors.toList());

        assertEquals(
                3,
                la.size()
        );
    }

    @Test
    public void keyEnvelopeDeserializingFunctionCanBeMapped() {
        List<Envelope.KeyEnvelope> a = Arrays.asList(
                Envelope.KeyEnvelope.newBuilder().build(),
                Envelope.KeyEnvelope.newBuilder().build(),
                Envelope.KeyEnvelope.newBuilder().build()
        );
        List<byte[]> la = a.stream()
                .map(Serdes.serializeKey)
                .collect(Collectors.toList());

        assertEquals(
                3,
                la.size()
        );
    }

    @Test
    public void keyEnvelopeDeserializerThrowsException() {
        assertThrows(Serdes.SerializationException.class, () -> Serdes.deserializeKey.apply(new byte[]{0,1,2,3,4,5}));
    }

    @Test
    public void keyEnvelopeKafkaDeserializerThrowsException() {
        assertThrows(org.apache.kafka.common.errors.SerializationException.class, () -> key_deserializer.deserialize("some-topic", new byte[]{0,1,2,3,4,5}));
    }

    @Test
    public void dataEnvelopeDeserializerThrowsException() {
        assertThrows(Serdes.SerializationException.class, () -> Serdes.deserializeValue.apply(new byte[]{0,1,2,3,4,5}));
    }

    @Test
    public void dataEnvelopeKafkaDeserializerThrowsException() {
        assertThrows(org.apache.kafka.common.errors.SerializationException.class, () -> data_deserializer.deserialize("some-topic", new byte[]{0,1,2,3,4,5}));
    }
}
