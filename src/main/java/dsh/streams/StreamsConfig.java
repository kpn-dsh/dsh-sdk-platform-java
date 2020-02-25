package dsh.streams;

import dsh.messages.DataStream;
import dsh.sdk.PkiProviderPikachu;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
     *
     * @param ds
     * @param cfg
     * @return
     */
    public static String keyFor(DataStream ds, ConfigType cfg) {
        return DATASTREAM_CONFIG_PREFIX + ds.fullName() + CONFIG_DELIMITER + cfg.configTypeValue;
    }

    /**
     *
     * @param cfg
     * @return
     */
    public static String valueFor(PartitionerType cfg) { return cfg.partitionerTypeValue; }

    /**
     *
     */
    public static Pattern validConfigEntryRegex = Pattern.compile("^datastream\\.(" + String.join("|", streamTypes()) + ")\\.[^.]+\\.[^.]+$");

    /**
     *
     * @param key
     * @return
     */
    public static Boolean isDataStreamConfig(String key) { return validConfigEntryRegex.asPredicate().test(key); }

    /**
     *
     * @param key
     * @return
     */
    public static String streamIdFromKey(String key) {
        return Optional.ofNullable(key)
                .filter(StreamsConfig::isDataStreamConfig)
                .map(k -> k.substring(DATASTREAM_CONFIG_PREFIX.length(), key.lastIndexOf(CONFIG_DELIMITER)))
                .orElseThrow(IllegalArgumentException::new);
    }

    /**
     *
     * @param ds
     * @param allProps
     * @return
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
     *
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
     *
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
