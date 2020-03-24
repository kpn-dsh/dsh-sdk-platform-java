package dsh.sdk.streams;

import dsh.messages.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Config property keys and utility functions
 */
public class StreamsConfig {
    private static final Logger logger = LoggerFactory.getLogger(StreamsConfig.class);

    private static final char CONFIG_DELIMITER = '.';
    private static final String DATASTREAM_CONFIG_PREFIX = "datastream" + CONFIG_DELIMITER;

    private static List<String> streamTypes() {
        return Arrays.stream(DataStream.StreamType.values())
                .map(DataStream.StreamType::prefix)
                .collect(Collectors.toList());
    }

    /**
     * generate the key as it is used in the platform properties for the given datastream.
     *
     * @param ds {@link DataStream} datastream
     * @param cfg the configurations sub property for that stream
     * @return the key to use in the properties
     */
    public static String keyFor(DataStream ds, ConfigType cfg) {
        return DATASTREAM_CONFIG_PREFIX + ds.fullName() + CONFIG_DELIMITER + cfg.configTypeValue;
    }

    /**
     * Get configuration properties value for a given partitioner
     *
     * @param cfg partitioner type
     * @return the value to use in the properties
     */
    public static String valueFor(PartitionerType cfg) { return cfg.partitionerTypeValue; }

    /**
     *
     */
    public static Pattern validConfigEntryRegex = Pattern.compile("^datastream\\.(" + String.join("|", streamTypes()) + ")\\.[^.]+\\.[^.]+$");

    /**
     * Regex pattern to match a datastream configuration entry
     *
     * @param key a given key from the platform properties
     * @return {@code true}   when it matches a datastream configuration entry
     *         {@code false}  when it doesn't
     */
    public static Boolean isDataStreamConfig(String key) { return validConfigEntryRegex.asPredicate().test(key); }

    /**
     * Get the DataStream identifier from a configuration entry.
     *
     * @param key a key from the platform properties
     * @throws IllegalArgumentException  when the given key doesn't match a datastream configuration entry
     * @return the stream identifier when it concerns a datastream config entry
     */
    public static String streamIdFromKey(String key) {
        return Optional.ofNullable(key)
                .filter(StreamsConfig::isDataStreamConfig)
                .map(k -> k.substring(DATASTREAM_CONFIG_PREFIX.length(), key.lastIndexOf(CONFIG_DELIMITER)))
                .orElseThrow(IllegalArgumentException::new);
    }

    /**
     * Get all configuration properties belonging to a specific datastream
     *
     * @param ds the datastream to look for
     * @param allProps full set of platform properties
     * @return datastream specific config
     */
    public static Map<String, String> configForStream(DataStream ds, Properties allProps) {
        return allProps.entrySet().stream()
                .filter(e -> isDataStreamConfig((String) e.getKey()))
                .filter(e -> ds.equals(DataStream.of(streamIdFromKey((String) e.getKey()))))
                .collect(Collectors.toMap(
                        e -> (String) e.getKey(),
                        e -> (String) e.getValue()
                ));
    }

    /**
     * possible sub configuration properties for a datastream
     */
    public enum ConfigType {
        PARTITIONER         ("partitioner"),
        PARTITIONINGDEPTH   ("partitioningDepth"),
        READ                ("read"),
        WRITE               ("write");

        private final String configTypeValue;
        ConfigType(String configValue) { this.configTypeValue = configValue; }
    }

    /**
     * list of supported partitioners
     */
    public enum PartitionerType {
        DEFAULT         ("default-partitioner"),
        TOPIC_LEVEL     ("topic-level-partitioner");

        private final String partitionerTypeValue;
        PartitionerType(String configValue) { this.partitionerTypeValue = configValue; }

        public static PartitionerType from(String n) {
            return Arrays.stream(PartitionerType.values()).filter(e -> e.partitionerTypeValue.equals(n)).findFirst().orElseThrow(IllegalArgumentException::new);
        }
    }
}
