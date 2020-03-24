import dsh.messages.Envelope;
import dsh.sdk.kafka.partitioners.DynamicStreamPartitioner;
import dsh.sdk.streams.StreamsConfigParser;
import mocks.MockKafka;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import utils.Loop;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class PartitionerTest {

    private Partitioner dynamicPartitioner = new DynamicStreamPartitioner();
    private Partitioner defaultPartitioner = new DefaultPartitioner();
    private Cluster mockCluster = MockKafka.clusterFor("stream.my-topic", 24);

    @Test
    @Tag("performance")
    public void DynamicStreamPartitionerFallbackPartitioningPerformance(TestReporter testReporter) {
        dynamicPartitioner.configure(Collections.emptyMap());

        long ops = new Loop(5, TimeUnit.SECONDS)
                .run(() -> dynamicPartitioner.partition("stream.my-topic", "1/2/3/4/5/6/7/8/9/0", "1/2/3/4/5/6/7/8/9/0".getBytes(), null, null, mockCluster))
                .ops();

        dynamicPartitioner.close();
        testReporter.publishEntry("dynamic-stream partitioning ops/s", Long.toString(ops));
    }

    @Test
    @Tag("performance")
    public void DefaultPartitionerPartitioningPerformance(TestReporter testReporter) {
        defaultPartitioner.configure(Collections.emptyMap());

        long ops = new Loop(5, TimeUnit.SECONDS)
                .run(() -> defaultPartitioner.partition("stream.my-topic", "1/2/3/4/5/6/7/8/9/0", "1/2/3/4/5/6/7/8/9/0".getBytes(), null, null, mockCluster))
                .ops();

        defaultPartitioner.close();
        testReporter.publishEntry("default partitioning ops/s", Long.toString(ops));
    }

    @Test
    public void DynamicStreamPartitionerShouldUseProperStreamConfig() {
        Properties props = new Properties();
        props.putAll(new HashMap<String, String>() {{
            // public stream
            put("datastream.stream.mine.partitions", "24");
            put("datastream.stream.mine.partitioner", "topic-level-partitioner");
            put("datastream.stream.mine.partitioningDepth", "6");
            // internal stream
            put("datastream.internal.mine.partitions", "24");
            put("datastream.internal.mine.partitioner", "topic-level-partitioner");
            put("datastream.internal.mine.partitioningDepth", "1");
        }});

        Partitioner p = new DynamicStreamPartitioner(StreamsConfigParser.of(props));
        Envelope.KeyEnvelope key = Envelope.KeyEnvelope.newBuilder().setKey("1/2/3/4/5/6/7/8/9/X").build();

        assertEquals(
                Utils.toPositive(Utils.murmur2("1/2/3/4/5/6".getBytes())) % 24,      // this is the partition key "1/2/3/4/5/6[/...]" that will end up on when using 24 partitions on the topic
                p.partition("stream.mine.xxx", key, null, null, null, MockKafka.clusterFor("stream.mine.xxx", 24))
        );

        assertEquals(
                Utils.toPositive(Utils.murmur2("1".getBytes())) % 24,      // this is the partition key "1[/...]" that will end up on when using 24 partitions on the topic
                p.partition("internal.mine.xxx", key, null, null, null, MockKafka.clusterFor("internal.mine.xxx", 24))
        );
    }
}
