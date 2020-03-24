package dsh.messages;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.function.Function;

/**
 * General purpose serializing and deserializing functions to operate on the protobuf Envelopes.
 *
 * The definition as {@code Function} of the Serdes makes it possible to directly use the in the Java Streams API:
 * <pre>{@code
 *
 * List<DataEnvelope> listOfDataEnvelopes = ...
 * listOfDataEnvelopes.stream()
 *      .map(Serdes.serializeKey)
 *      .forEach( ... )
 * }</pre>
 */
public class Serdes {

    /**
     * Exception to indicate something went wrong while serializing/deserializing the raw data
     * into Key- and/or DataEnvelopes.
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
     * Function to deserialize a KeyEnvelope
     * @see dsh.messages.Envelope.KeyEnvelope
     * @see Serdes#fromBytes(Parser, byte[])
     */
    public static Function<byte[], Envelope.KeyEnvelope> deserializeKey = (byte[] bytes) -> Serdes.fromBytes(Envelope.KeyEnvelope.parser(), bytes);

    /**
     * Function to deserialize a DataEnvelope
     * @see dsh.messages.Envelope.DataEnvelope
     * @see Serdes#fromBytes(Parser, byte[])
     */
    public static Function<byte[], Envelope.DataEnvelope> deserializeValue = (byte[] bytes) -> Serdes.fromBytes(Envelope.DataEnvelope.parser(), bytes);

    /**
     * Function to serialize a KeyEnvelope
     * @see dsh.messages.Envelope.KeyEnvelope
     * @see Serdes#toBytes(Message)
     */
    public static Function<Envelope.KeyEnvelope, byte[]> serializeKey = Serdes::toBytes;

    /**
     * Function to serialize a DataEnvelope
     * @see dsh.messages.Envelope.DataEnvelope
     * @see Serdes#toBytes(Message)
     */
    public static Function<Envelope.DataEnvelope, byte[]> serializeValue = Serdes::toBytes;
}
