package dsh.messages;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DataStream implements Comparable<DataStream> {
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
            return  Arrays.stream(StreamType.values()).filter(e -> e.prefix.equals(prefix)).findFirst().orElseThrow(IllegalArgumentException::new);
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

    /**
     *
     * @param s
     * @return
     */
    public static DataStream parse(String s) {
        if (s == null) return null;

        List<String> parts = (s.contains(String.valueOf(colon)))
                ? Utils.topicSplit(s, colon)
                :  Utils.topicSplit(s, dot);

        return new DataStream(StreamType.fromPrefix(parts.get(0)), parts.get(1));
    }

    private static final char dot = '.';
    private static final char colon = ':';
}
