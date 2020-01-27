package dsh.internal;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 */
public class StringUtils {
    /**
     * Default topic delimiter
     */
    public static final String TOPIC_DELIMITER = "/";

    /**
     *
     * @param delim  the delimiter to use to identify the individual sub-groups in the string to split on
     * @param key    the key string
     * @param level  the number of sub-groups that need to be collected from the string
     *
     * @return substring containing the given number of subgroups concat'ed together
     *
     * <pre>
     *     takeFlat(TOPIC_DELIMITER, "0/1/2/3/4/5", 3)  --> gives: "012"
     * </pre>
     */
    public static String takeFlat(String delim, String key, int level) {
        return Optional.ofNullable(key).map(s -> Arrays.stream(s.split(delim)).limit(level).collect(Collectors.joining())).orElse(null);
    }
}
