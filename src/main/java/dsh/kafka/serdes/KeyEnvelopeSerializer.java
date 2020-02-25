package dsh.kafka.serdes;

import dsh.messages.Envelope;
import dsh.messages.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka KeyEnvelope serializer
 */
public class KeyEnvelopeSerializer implements Serializer<Envelope.KeyEnvelope> {

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Envelope.KeyEnvelope data) {
        try {
            return Serdes.serializeKey.apply(data);
        } catch (Serdes.SerializationException e) {
            throw new SerializationException(e);
        }
    }
}
