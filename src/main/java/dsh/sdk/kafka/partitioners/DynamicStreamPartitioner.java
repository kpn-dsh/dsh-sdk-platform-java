package dsh.sdk.kafka.partitioners;

import dsh.messages.Envelope;
import dsh.sdk.streams.StreamsConfigParser;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * A Kafka Partitioner implementation that uses the generic {@link DshStreamPartitioner}
 */
public class DynamicStreamPartitioner implements Partitioner {

    private DshStreamPartitioner underlying;
    private final Partitioner defaultPartitioner = new DefaultPartitioner();
    public static final String CONFIG_KEY = "__dynamicstreampartitioner";

    /**
     * This constructor will not be used when this partitioner gets configured in the producer properties.
     * It is only valid for testing purposes, or for usage in Frameworks that let you construct the Kafka partitioner yourself.
     *
     * @param parser the StreamsParser to use for metadata queries
     */
    public DynamicStreamPartitioner(StreamsConfigParser parser) {
        underlying = new DshStreamPartitioner(parser);
    }

    /**
     * Default constructor
     *
     * This requires the {@code configure} function to be called with the proper configuration properties
     * so that a StreamsParser can be constructed from them.
     */
    public DynamicStreamPartitioner() {
        this.underlying = null;
    }

    @Override
    public void close() { defaultPartitioner.close(); }

    /**
     * initializes the Partitioner
     *
     * @param configs config properties to create a StreamsParser from
     */
    @Override
    public void configure(Map<String, ?> configs) {
        if(underlying == null && configs.containsKey(CONFIG_KEY)) underlying = new DshStreamPartitioner((StreamsConfigParser)configs.get(CONFIG_KEY));
        defaultPartitioner.configure(configs);
    }

    /**
     * Standard Kafka partitioning function (called from the Kafka client library)
     * Computes the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keyStr = key instanceof Envelope.KeyEnvelope ? ((Envelope.KeyEnvelope)key).getKey()
                                                            : key instanceof String ? (String)key
                                                                                    : new String(keyBytes);

        return (underlying != null)
                ? underlying.partition(
                                topic,
                                keyStr,
                                cluster.partitionCountForTopic(topic),
                                (unused1, unused2) -> defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster)
                            )
                : defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }
}
