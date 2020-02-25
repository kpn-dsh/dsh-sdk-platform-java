package dsh.messages;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.function.Function;

/**
 * General purpose serializing and deserializing functions
 * to operate on the protobuf Envelopes
 */
public class Serdes {

    /**
     *
     */
    public static class SerializationException extends RuntimeException {
        private static final long serialVersionUID = 4023244989841487397L;
        public SerializationException(Throwable cause) { super(cause); }
    }

    private static <T extends Message> T fromBytes(Parser<T> parser, byte[] bytes) {
        if(bytes == null) return null;
        try {
            return parser.parseFrom(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    private static <T extends Message> byte[] toBytes(T msg) {
        if(msg == null) return null;
        try {
            return msg.toByteArray();
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    /**
     *
     */
    public static Function<byte[], Envelope.KeyEnvelope> deserializeKey = (byte[] bytes) -> Serdes.fromBytes(Envelope.KeyEnvelope.parser(), bytes);


    /**
     *
     */
    public static Function<byte[], Envelope.DataEnvelope> deserializeValue = (byte[] bytes) -> Serdes.fromBytes(Envelope.DataEnvelope.parser(), bytes);

    /**
     *
     */
    public static Function<Envelope.KeyEnvelope, byte[]> serializeKey = Serdes::toBytes;

    /**
     *
     */
    public static Function<Envelope.DataEnvelope, byte[]> serializeValue = Serdes::toBytes;
}
