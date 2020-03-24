package dsh.messages;

import dsh.sdk.internal.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * The {@code DataStream} class represents an abstract "stream".
 * <p>
 * This class basically provides all functions needed to convert between kafka topics and (abstract) streams, back and forth.
 * There exist 2 different kind of streams on the DSH:
 * <ul>
 *     <li>PUBLIC streams</li>
 *     <li>INTERNAL streams</li>
 * </ul>
 * </p>
 *
 * The idea in a DSH application is that on application level (business logic) you only work with abstract streams {@link DataStream}
 * and only convert to a topic at the lowest level, directly before producing to Kafka.
 */
public class DataStream implements Comparable<DataStream>, Serializable {
    private static final long serialVersionUID = 4385793608612059488L;

    @Override
    public int compareTo(DataStream that) {
        return (this.type.prefix() + this.name).compareTo(that.type.prefix() + that.name);
    }

    @Override
    public int hashCode() {
        return (this.type.prefix() + this.name).hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if(this == that) return true;
        if(! (that instanceof DataStream)) return false;
        return 0 == this.compareTo((DataStream)that);
    }

    /**
     * Possible types of streams (i.e. the prefix used in the Kafka topic)
     * <ul>
     *     <li>"stream"   --> identifies a PUBLIC stream</li>
     *     <li>"internal" --> cross tenant INTERNAL stream</li>
     * </ul>
     */
    public enum StreamType {
        PUBLIC   ( "stream"   ),
        INTERNAL ( "internal" );

        private final String prefix;
        StreamType(String prefix) { this.prefix = prefix; }
        public String prefix() { return this.prefix; }

        public static StreamType fromPrefix(String prefix) {
            return  Arrays.stream(StreamType.values()).filter(e -> e.prefix.equals(prefix.trim().toLowerCase())).findFirst().orElseThrow(IllegalArgumentException::new);
        }
    }

    private final StreamType type;
    private final String name;
    private DataStream() { throw new AssertionError(); }

    /**
     * Internal constructor
     * Create a DataStream object from the given {@see StreamType} and abstract name.
     *
     * Use the static builder functions to create DataStream objects
     * @see #of(String)
     * @see #of(String, String)
     * @see #of(StreamType, String)
     *
     * @param type  the {@link StreamType} of the stream
     * @param name the abstract name for the stream
     */
    private DataStream(StreamType type, String name) {
        this.type = type;
        this.name = name;
    };

    /**
     * The type of the stream
     *
     * @return one of the {@link StreamType} enum values
     * @see StreamType
     */
    public StreamType type() { return this.type; }

    /**
     * The abstract name of the stream,
     * which is basically the full stream name without the type identifier.
     *
     * @return abstract stream name
     * @see #fullName() fullName
     */
    public String name() { return this.name; }

    /**
     * Create the Kafka topic name that would be used when the provided tenant would write to this stream.
     *
     * @param tenant  the tenant identifier
     * @return the name of the Kafka topic that will be used under the hood when the provided
     *         tenant would write to that stream.
     */
    public String asTopic(String tenant) {
        return this.type.prefix() + dot + this.name + dot + tenant;
    }

    /**
     * String representation of the DataStream object
     *
     * @return String  format: \<stream-type\>:\<stream-name\>
     */
    @Override
    public String toString() {
        return this.type.prefix() + colon + this.name;
    }

    /**
     * String representation of the DataStream object
     *
     * @return String  format: \<stream-type\>.\<stream-name\>
     */
    public String fullName() {
        return this.type.prefix() + dot + this.name;
    }

    /**
     * Create a DataStream object from the stream representation given.
     *
     * 3 formats are supported when parsing the given string:
     * 1) [internal|stream].(stream-name)                 {@link #fullName() fullName} representation of the stream.
     * 2) [internal|stream]:(stream-name)                 {@link #toString()} toString} default String representation of the stream.
     * 3) [internal|stream].(stream-name).(tenant-id)     {@link #asTopic(String) asTopic} full tenant-topic representation of a topic belonging to the stream.
     *
     * @param   text        textual (abstract or topic) representation of a stream
     * @return  valid object when parsing the textual representation succeeded.
     * @exception IllegalArgumentException when the string representation can not be parsed into a valid {@code DataStream} object.
     */
    public static DataStream of(String text) {
        if (text == null) return null;

        List<String> parts = (text.contains(String.valueOf(colon)))
                ? StringUtils.topicSplit(text, colon)
                : StringUtils.topicSplit(text, dot);

        if(parts.size() < 2 || parts.get(0).trim().isEmpty() || parts.get(1).trim().isEmpty()) throw new IllegalArgumentException();
        return new DataStream(StreamType.fromPrefix(parts.get(0).trim()), parts.get(1).trim());
    }

    /**
     * Create a DataStream object from the given {@link StreamType} and abstract name.
     *
     * @param typ  the {@link StreamType type} of the stream
     * @param name the abstract name for the stream
     * @return valid {@code DataStream} object
     */
    public static DataStream of(StreamType typ, String name) { return new DataStream(typ, name); }

    /**
     * Create a DataStream object from the given {@link StreamType} and abstract name.
     *
     * @param typ  the {@link StreamType type} of the stream
     * @param name the abstract name for the stream
     * @return valid {@code DataStream} object
     * @exception IllegalArgumentException when the given stream-type can not be parsed to a valid {@link StreamType StreamType}
     */
    public static DataStream of(String typ, String name) { return new DataStream(StreamType.fromPrefix(typ), name); }

    private static final char dot = '.';
    private static final char colon = ':';
}
