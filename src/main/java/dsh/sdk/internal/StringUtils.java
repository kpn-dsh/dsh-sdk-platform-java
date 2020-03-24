package dsh.sdk.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * General purpose String utility functions
 */
public class StringUtils {
    /**
     * Default topic delimiter for MQTT style topics
     */
    public static final String TOPIC_DELIMITER = "/";

    /**
     * "Flatten" an MQTT style topic up til a certain depth.
     *
     * @param delim  the delimiter to use to identify the individual sub-groups in the string to split on
     * @param key    the key string
     * @param level  the number of sub-groups that need to be collected from the string
     *
     * @return substring containing the given number of subgroups concat'ed together
     *
     * <pre>{@code
     *     takeFlat(TOPIC_DELIMITER, "0/1/2/3/4/5", 3)  --> gives: "012"
     * }</pre>
     */
    public static String takeFlat(String delim, String key, int level) {
        return Optional.ofNullable(key).map(s -> Arrays.stream(s.split(delim)).limit(level).collect(Collectors.joining())).orElse(null);
    }

    /**
     * Truncate an MQTT style topic up til a certain depth.
     *
     * @param delim  the delimiter to use to identify the individual sub-groups in the string to split on
     * @param key    the key string
     * @param level  the number of sub-groups that need to be collected from the string
     *
     * @return substring containing the given number of subgroups concat'ed together
     *
     * <pre>{@code
     *     take(TOPIC_DELIMITER, "0/1/2/3/4/5", 3)  --> gives: "0/1/2"
     * }</pre>
     */
    public static String take(String delim, String key, int level) {
        return Optional.ofNullable(key).map(s -> Arrays.stream(s.split(delim)).limit(level).collect(Collectors.joining(delim))).orElse(null);
    }

    /**
     * Split a topic style string into its individual parts.
     *
     * A topic looking like: <i>type.name.tenant</i> will result in: {@code List(type, name, tenant)}
     * A topic looking like: <i>type.my.name.with.dots.tenant</i> will result in: {@code List(type, my.name.with.dots, tenant)}
     *
     * @param source  the source topic
     * @param delim   the delimiter used to identify the individual levels in the topic string
     * @return List of individual levels of the topic
     */
    public static List<String> topicSplit(String source, char delim) {
        if(source == null) throw new IllegalArgumentException();

        int firstDot = source.indexOf(delim);
        if (firstDot == -1) throw new IllegalArgumentException();
        else {
            int lastDot = source.lastIndexOf(delim);
            if (lastDot != firstDot) return Arrays.asList(source.substring(0, firstDot), source.substring(firstDot + 1, lastDot), source.substring(lastDot + 1));
            else return Arrays.asList(source.substring(0, firstDot), source.substring(firstDot + 1), null);
        }
    }
}
