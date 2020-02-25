package dsh.kafka.partitioners;

import dsh.messages.Envelope;
import dsh.streams.StreamsParser;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 *
 */
public class DynamicStreamPartitioner implements Partitioner {

    private DshStreamPartitioner underlying;
    private final Partitioner defaultPartitioner = new DefaultPartitioner();

    /**
     *
     * @param parser
     */
    public DynamicStreamPartitioner(StreamsParser parser) {
        underlying = new DshStreamPartitioner(parser);
    }

    /**
     *
     */
    public DynamicStreamPartitioner() {
        this.underlying = null;
    }

    @Override
    public void close() { defaultPartitioner.close(); }

    @Override
    public void configure(Map<String, ?> configs) {
        if(underlying == null) underlying = new DshStreamPartitioner(StreamsParser.of(configs));
        defaultPartitioner.configure(configs);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keyStr = key instanceof Envelope.KeyEnvelope ? ((Envelope.KeyEnvelope)key).getKey()
                                                            : key instanceof String ? (String)key
                                                                                    : new String(keyBytes);

        return underlying.partition(
                topic,
                keyStr,
                cluster.partitionCountForTopic(topic),
                (unused1, unused2) -> defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster)
        );
    }
}
