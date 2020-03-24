package dsh.sdk.kafka.serdes;

import dsh.messages.Envelope;
import dsh.messages.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Kafka DataEnvelope deserializer
 */
public class DataEnvelopeDeserializer implements Deserializer<Envelope.DataEnvelope> {

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Envelope.DataEnvelope deserialize(String topic, byte[] data) {
        try {
            return Serdes.deserializeValue.apply(data);
        } catch (Serdes.SerializationException e) {
            throw new SerializationException(e.getCause());
        }
    }
}
