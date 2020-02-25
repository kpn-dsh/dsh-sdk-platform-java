package dsh.streams;

import dsh.internal.StringUtils;
import dsh.messages.DataStream;
import dsh.sdk.Sdk;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class StreamsParser {
    private static final Logger logger = LoggerFactory.getLogger(StreamsParser.class);

    public interface PartitionerFunction extends BiFunction<String, Integer, Integer>, Serializable {};

    /**
     *
     */
    public static class StreamContract implements Serializable {
        private static final long serialVersionUID = -4281114556553196482L;

        public StreamContract(DataStream ds, PartitionerFunction partitioner, String subscribePattern, String produceTopic) {
            this.datastream = ds;
            this.partitioner = partitioner;
            this.subscribePattern = subscribePattern != null ? Pattern.compile(subscribePattern) : null;
            this.produceTopic = produceTopic;
        }

        private final DataStream datastream;
        private final PartitionerFunction partitioner;
        private final Pattern subscribePattern;
        private final String produceTopic;

        public DataStream datastream() { return datastream; }
        public BiFunction<String, Integer, Integer> partitioner() { return partitioner; }
        public Optional<Pattern> subscribePattern() { return Optional.ofNullable(subscribePattern); }
        public Optional<String> produceTopic() { return Optional.ofNullable(produceTopic); }

        @Override
        public String toString() {
            return String.format(
                    "streamcontract (datastream: %s, sub-pattern: %s, pub-topic: %s)",
                    datastream.toString(),
                    subscribePattern != null? subscribePattern.pattern() : "<none>",
                    produceTopic != null ? produceTopic : "<none>"

            );
        }
    }

    /**
     *
     * @param sdk
     * @return
     */
    public static StreamsParser of(Sdk sdk) { return new StreamsParser(sdk.getProps()); }

    /**
     *
     * @param props
     * @return
     */
    public static StreamsParser of(Properties props) { return new StreamsParser(props); }

    /**
     *
     * @param map
     * @return
     */
    public static StreamsParser of(Map<String, ?> map) {
        Properties props = new Properties();
        logger.error("config keys: {}", String.join(",", map.keySet()));
        props.putAll(map);
        return new StreamsParser(props);
    }

    //
    private final Map<DataStream, StreamContract> streamProperties;

    //
    private StreamsParser(Properties props) {
        // we first create a lookup table to make looking up streams faster

        Stream<DataStream> allStreams = props.keySet().stream()
                .map(Object::toString)
                .filter(StreamsConfig.validConfigEntryRegex.asPredicate())
                .map(StreamsConfig::streamIdFromKey)
                .distinct()
                .map(DataStream::of);

        Map<DataStream, Map<String, String>> allStreamsWithProps = allStreams.collect(Collectors.toMap(
                ds -> ds,
                ds -> StreamsConfig.configForStream(ds, props)
        ));

        streamProperties = allStreamsWithProps.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new StreamContract(
                                e.getKey(),
                                propsToPartitioner(e.getKey(), e.getValue()),
                                (String) props.get(StreamsConfig.keyFor(e.getKey(), StreamsConfig.ConfigType.READ)),
                                (String) props.get(StreamsConfig.keyFor(e.getKey(), StreamsConfig.ConfigType.WRITE))
                        )
                ));
    }

    //
    private static PartitionerFunction propsToPartitioner(DataStream ds, Map<String, String> props) {
        if(StreamsConfig.PartitionerType.from(props.get(StreamsConfig.keyFor(ds, StreamsConfig.ConfigType.PARTITIONER))) == StreamsConfig.PartitionerType.TOPIC_LEVEL) {
            int depth = Integer.parseInt(props.get(StreamsConfig.keyFor(ds, StreamsConfig.ConfigType.PARTITIONINGDEPTH)));
            return (key, numPartitions) -> Utils.toPositive(Utils.murmur2(StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, key, depth).getBytes(StandardCharsets.UTF_8))) % numPartitions;
        }
        else
            return (key, numPartitions) -> Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;
    }

    /**
     *
     * @param dataStreams
     * @return
     */
    public Pattern subscriptionPatternFor(DataStream... dataStreams) {
        Supplier<String> concatenatePatterns = () -> Arrays.stream(dataStreams)
                .map(streamProperties::get)
                .filter(Objects::nonNull)
                .map(StreamContract::subscribePattern)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Pattern::pattern)
                .collect(Collectors.joining("|"));

        return Optional.of(concatenatePatterns.get())
                .filter(e -> ! e.isEmpty())
                .map(Pattern::compile)
                .orElseThrow(NoSuchElementException::new);
    }

    /**
     *
     * @param ds
     * @return
     */
    public Optional<StreamContract> findStream(DataStream ds) {
        logger.error("stream contracts: " + streamProperties.keySet().stream().map(DataStream::fullName).collect(Collectors.joining(",")));
        logger.error("contract for {} : {}", ds.toString(), streamProperties.get(ds));

        return Optional.ofNullable(streamProperties.get(ds));
    }
}
