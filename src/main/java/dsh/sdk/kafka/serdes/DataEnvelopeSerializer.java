package dsh.sdk.kafka.serdes;

import dsh.messages.Envelope;
import dsh.messages.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka DataEnvelope serializer
 */
public class DataEnvelopeSerializer implements Serializer<Envelope.DataEnvelope> {

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Envelope.DataEnvelope data) {
        try {
            return Serdes.serializeValue.apply(data);
        } catch (Serdes.SerializationException e) {
            throw new SerializationException(e);
        }
    }
}
