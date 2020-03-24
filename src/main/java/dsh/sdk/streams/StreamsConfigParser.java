package dsh.sdk.streams;

import dsh.messages.DataStream;
import dsh.sdk.Sdk;
import dsh.sdk.internal.StringUtils;
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
 * Utility class to query all Stream related config from to be used in the application.
 *
 * The StreamsParser typically is configured from the Sdk object.
 * <pre>{@code
 *   Sdk sdk = new Sdk.Builder().autoDetect().build()
 *   StreamsParser parser = StreamsParser.of(sdk)
 * }</pre>
 */
public class StreamsConfigParser {
    private static final Logger logger = LoggerFactory.getLogger(StreamsConfigParser.class);
    private StreamsConfigParser() { throw new AssertionError(); }

    /**
     * Identifies a partitioner function type
     *
     * {@code Integer function(topicName, numPartitions)}
     */
    public interface PartitionerFunction extends BiFunction<String, Integer, Integer>, Serializable {}
    private static final PartitionerFunction defaultPartitioning = (key, numPartitions) -> Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;

    /**
     * "Contract" properties for a stream
     * - the partitioner function to use when producing on that stream
     * - the kafka topic you need to produce on when you want to write data to the stream
     * - the subscription pattern to use when you want to consume from the stream
     */
    public static class StreamContract implements Serializable {
        private static final long serialVersionUID = -4281114556553196482L;

        // can only be used from within the parser
        private StreamContract(DataStream ds, PartitionerFunction partitioner, String subscribePattern, String produceTopic) {
            this.datastream = ds;
            this.partitioner = partitioner;
            this.subscribePattern = subscribePattern != null ? Pattern.compile(subscribePattern) : null;
            this.produceTopic = produceTopic;
        }

        private final DataStream datastream;
        private final PartitionerFunction partitioner;
        private final Pattern subscribePattern;
        private final String produceTopic;

        // the DataStream
        public DataStream datastream() { return datastream; }

        // the partitioner function to use when producing on that stream
        public BiFunction<String, Integer, Integer> partitioner() { return partitioner; }

        // the subscription pattern to use when you want to consume from the stream
        // or NONE when consuming from the stream is not allowed.
        public Optional<Pattern> subscribePattern() { return Optional.ofNullable(subscribePattern); }

        // the kafka topic you need to produce on when you want to write data to the stream
        // or NONE when producing to the stream is not allowed.
        public Optional<String> produceTopic() { return Optional.ofNullable(produceTopic); }

        @Override
        public String toString() {
            return String.format(
                    "streamcontract [%x] (datastream: %s, sub-pattern: %s, pub-topic: %s)",
                    this.hashCode(),
                    datastream.toString(),
                    subscribePattern != null? subscribePattern.pattern() : "<none>",
                    produceTopic != null ? produceTopic : "<none>"

            );
        }
    }

    /**
     * Create a new StreamsParser object from the given SDK object.
     *
     * @param sdk the main SDK object to initialize this stream-parser from
     * @return a new StreamsParser object
     */
    public static StreamsConfigParser of(Sdk sdk) { return new StreamsConfigParser(sdk.getProps()); }

    /**
     * Create a new StreamsParser object from a given set of properties.
     *
     * @param props properties from which to initialize the StreamsParser
     * @return a new StreamsParser object
     */
    public static StreamsConfigParser of(Properties props) { return new StreamsConfigParser(props); }

    /**
     * Create a new StreamsParser object from a given set of properties.
     *
     * @param map properties from which to initialize the StreamsParser
     * @return a new StreamsParser object
     */
    public static StreamsConfigParser of(Map<String, ?> map) {
        Properties props = new Properties();
        logger.debug("config keys: {}", String.join(",", map.keySet()));
        props.putAll(map);
        return new StreamsConfigParser(props);
    }

    // lookup table
    private final Map<DataStream, StreamContract> streamProperties;

    private StreamsConfigParser(Properties props) {
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
        PartitionerFunction partitioningFunction = defaultPartitioning;

        try {
            if (StreamsConfig.PartitionerType.from(props.get(StreamsConfig.keyFor(ds, StreamsConfig.ConfigType.PARTITIONER))) == StreamsConfig.PartitionerType.TOPIC_LEVEL) {
                int depth = Integer.parseInt(props.get(StreamsConfig.keyFor(ds, StreamsConfig.ConfigType.PARTITIONINGDEPTH)));
                partitioningFunction = (key, numPartitions) -> Utils.toPositive(Utils.murmur2(StringUtils.take(StringUtils.TOPIC_DELIMITER, key, depth).getBytes(StandardCharsets.UTF_8))) % numPartitions;
            }
        }
        catch(IllegalArgumentException e) { /* config not found or wrong partitioner config -- we stick to the defaultPartitioning function */}
        return partitioningFunction;
    }

    /**
     * Creates the subscription pattern needed for the Kafka consumer
     * to consume from all provided streams.
     *
     * @param dataStreams list of {@link DataStream datastream} objects you wish to consume from
     * @return Regex Pattern to give to the {@link org.apache.kafka.clients.consumer.Consumer#subscribe(Pattern) subscribe} function.
     * @see DataStream
     */
    public Pattern subscriptionPatternFor(Collection<DataStream> dataStreams) {
        return subscriptionPatternFor(dataStreams.toArray(new DataStream[0]));
    }

    /**
     * Creates the subscription pattern needed for the Kafka consumer
     * to consume from all provided streams.
     *
     * @param dataStreams list of {@link DataStream datastream} objects you wish to consume from
     * @return Regex Pattern to give to the {@link org.apache.kafka.clients.consumer.Consumer#subscribe(Pattern) subscribe} function.
     * @see DataStream
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
     * Find a streams contract information.
     *
     * @param ds the {@link DataStream datastream} to search the contract for.
     * @return  <b>None</b> when no info for the stream is found
     *          <b>Some</b> with the stream contract
     * @see DataStream
     * @see StreamContract
     */
    public Optional<StreamContract> findStream(DataStream ds) {
        return Optional.ofNullable(streamProperties.get(ds));
    }

    @Override
    public String toString() {
        return String.format(
                "streamsparser [%x] (streamcount: %d)",
                this.hashCode(),
                streamProperties.size()
        );
    }
}
