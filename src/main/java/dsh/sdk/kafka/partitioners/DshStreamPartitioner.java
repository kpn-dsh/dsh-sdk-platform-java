package dsh.sdk.kafka.partitioners;

import dsh.messages.DataStream;
import dsh.sdk.streams.StreamsConfigParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * {@code DshStreamPartitioner} is a generic partitioning implementation specifically for usage within the DSH.
 *
 * The partitioner will maintain a list of topics with their corresponding required partitioning functions,
 * depending on the stream metadata it gets from the (@StreamsParser}.
 * When no partitioning info would be found for the stream (e.g. it concerns a scratch topic or an internal stream)
 * one can give it a default partitioning function to fallback to.  Once this function is hit, it will be added to
 * the metadata cache and be used for all subsequent publishes on that same topic.
 */
public class DshStreamPartitioner {
    private final StreamsConfigParser parser;
    private Map<String, BiFunction<String, Integer, Integer>> partitionerFunctions = new HashMap<>();

    /**
     * Constructor
     *
     * The given parser would be the StreamsParser that got created from the Sdk.
     * <pre>{@code
     *   new DshStreamPartitioner(StreamsParser.of(sdk))
     * }</pre>
     *
     * @param parser The StreamsParser to use to query for the stream metadata
     */
    public DshStreamPartitioner(StreamsConfigParser parser) {
        this.parser = parser;
    }

    /**
     * Calculates the partition to produce on for a given Key.
     *
     * The partitioning functions will automatically take the stream metadata into account
     * (from the given {@code StreamsParser}, and cache this info for later use (to optimize performance)
     * When no matching metadata is found in the StreamsParser for the given topic, an IllegalArgumentException is thrown.
     *
     * @param topic the kafka topic you wich to produce to
     * @param key the key (from the KeyEnvelope) of the data to produce to Kafka
     * @param numPartitions the number of partitions of the Kafka topic
     * @return partition number for this key on that topic
     */
    public int partition(String topic, String key, int numPartitions) {
        return partition(topic, key, numPartitions, (unused1, unused2) -> { throw new IllegalArgumentException("no valid partitioner found for topic"); });
    }

    /**
     * Calculates the partition to produce on for a given Key.
     *
     * The partitioning functions will automatically take the stream metadata into account
     * (from the given {@code StreamsParser}, and cache this info for later use (to optimize performance)
     * When no matching metadata is found in the StreamsParser for the given topic, the (@defaultPartitioner} will be used
     * (and cached!) to calculate the partition.
     *
     * @param topic the kafka topic you wich to produce to
     * @param key the key (from the KeyEnvelope) of the data to produce to Kafka
     * @param numPartitions the number of partitions of the Kafka topic
     * @param defaultPartitioner the partitioning function to use when no metadata is found for the topic
     * @return partition number for this key on that topic
     */
    public int partition(String topic, String key, int numPartitions, BiFunction<String, Integer, Integer> defaultPartitioner) {
        BiFunction<String, Integer, Integer> pF = partitionerFunctions.computeIfAbsent(
                topic,
                k -> parser.findStream(DataStream.of(topic)).map(StreamsConfigParser.StreamContract::partitioner).orElse(
                        defaultPartitioner   // this should be the exception for this kind of partitioner
                )
        );

        return pF.apply(key, numPartitions);
    }
}
