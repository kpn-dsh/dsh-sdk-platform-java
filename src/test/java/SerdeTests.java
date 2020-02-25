import com.google.protobuf.ByteString;
import dsh.kafka.serdes.DataEnvelopeDeserializer;
import dsh.kafka.serdes.DataEnvelopeSerializer;
import dsh.kafka.serdes.KeyEnvelopeDeserializer;
import dsh.kafka.serdes.KeyEnvelopeSerializer;
import dsh.messages.Envelope;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestReporter;
import utils.Loop;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
