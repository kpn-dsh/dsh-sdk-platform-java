package mocks;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockKafka {

    public static Cluster clusterFor(String topic, Integer numPartitions) {
        Node broker = new Node(1001, "localhost", 9091);

        List<PartitionInfo> partitionInfo = IntStream.range(0, numPartitions)
                .mapToObj(n -> new PartitionInfo(topic, n, broker, new Node[0], new Node[0]))
                .collect(Collectors.toList());

        return new Cluster(
                "mock-kafka-cluster",
                Collections.singletonList(broker),
                partitionInfo,
                Collections.emptySet(),
                Collections.emptySet()
        );
    }

}
