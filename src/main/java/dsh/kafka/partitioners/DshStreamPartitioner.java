package dsh.kafka.partitioners;

import dsh.messages.DataStream;
import dsh.messages.Envelope;
import dsh.streams.StreamsParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class DshStreamPartitioner {
    private final StreamsParser parser;
    private Map<String, BiFunction<String, Integer, Integer>> partitionerFunctions = new HashMap<>();

    /**
     *
     * @param parser
     */
    public DshStreamPartitioner(StreamsParser parser) {
        this.parser = parser;
    }

    /**
     *
     * @param topic
     * @param key
     * @param numPartitions
     * @return
     */
    public int partition(String topic, String key, int numPartitions) {
        return partition(topic, key, numPartitions, (unused1, unused2) -> { throw new IllegalArgumentException("no valid partitioner found for topic"); });
    }

    public int partition(String topic, String key, int numPartitions, BiFunction<String, Integer, Integer> defaultPartitioner) {
        BiFunction<String, Integer, Integer> pF = partitionerFunctions.computeIfAbsent(
                topic,
                k -> parser.findStream(DataStream.of(topic)).map(StreamsParser.StreamContract::partitioner).orElse(
                        defaultPartitioner   // this should be the exception for this kind of partitioner
                )
        );

        return pF.apply(key, numPartitions);
    }
}
