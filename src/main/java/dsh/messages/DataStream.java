package dsh.messages;

import dsh.internal.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DataStream implements Comparable<DataStream>, Serializable {
    private static final long serialVersionUID = 4385793608612059488L;

    @Override
    public int compareTo(DataStream that) {
        return (this.type.prefix() + dot + this.name).compareTo(that.type.prefix() + dot + that.name);
    }

    @Override
    public int hashCode() {
        return (this.type.prefix() + dot + this.name).hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if(this == that) return true;
        if(! (that instanceof DataStream)) return false;
        return 0 == this.compareTo((DataStream)that);
    }

    /**
     *
     */
    public enum StreamType {
        PUBLIC   ( "stream"   ),
        INTERNAL ( "internal" ),
        SCRATCH  ( "scratch"  );

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
     *
     * @param type
     * @param name
     */
    public DataStream(StreamType type, String name) {
        this.type = type;
        this.name = name;
    };

    /**
     *
     * @return
     */
    public StreamType type() { return this.type; }

    /**
     *
     * @return
     */
    public String name() { return this.name; }

    /**
     *
     * @param tenant
     * @return
     */
    public String asTopic(String tenant) {
        return this.type.prefix() + dot + this.name + dot + tenant;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return this.type.prefix() + colon + this.name;
    }

    public String fullName() {
        return this.type.prefix() + dot + this.name;
    }

    /**
     *
     * @param s
     * @return
     */
    public static DataStream of(String s) {
        if (s == null) return null;

        List<String> parts = (s.contains(String.valueOf(colon)))
                ? StringUtils.topicSplit(s, colon)
                : StringUtils.topicSplit(s, dot);

        if(parts.size() < 2 || parts.get(0).trim().isEmpty() || parts.get(1).trim().isEmpty()) throw new IllegalArgumentException();
        return new DataStream(StreamType.fromPrefix(parts.get(0).trim()), parts.get(1).trim());
    }

    /**
     *
     * @param typ
     * @param name
     * @return
     */
    public static DataStream of(StreamType typ, String name) { return new DataStream(typ, name); }

    /**
     *
     * @param typ
     * @param name
     * @return
     */
    public static DataStream of(String typ, String name) { return new DataStream(StreamType.fromPrefix(typ), name); }

    private static final char dot = '.';
    private static final char colon = ':';
}
