package dsh.sdk.kafka.serdes;

import dsh.messages.Envelope;
import dsh.messages.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Kafka KeyEnvelope deserializer
 */
public class KeyEnvelopeDeserializer implements Deserializer<Envelope.KeyEnvelope> {

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Envelope.KeyEnvelope deserialize(String topic, byte[] data) {
        try {
            return Serdes.deserializeKey.apply(data);
        } catch (Serdes.SerializationException e) {
            throw new SerializationException(e.getCause());
        }
    }
}
