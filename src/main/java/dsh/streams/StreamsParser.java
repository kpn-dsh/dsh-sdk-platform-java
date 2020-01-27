package dsh.streams;

import dsh.messages.DataStream;
import dsh.pki.Pki;
import dsh.internal.StringUtils;
import org.apache.kafka.common.utils.Utils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 *
 */
public class StreamsParser {
    public static class StreamProps {
        public StreamProps(DataStream ds, BiFunction<String, Integer, Integer> partitioner) {
            this.datastream = ds;
            this.partitioner = partitioner;
        }

        private final DataStream datastream;
        private final BiFunction<String, Integer, Integer> partitioner;

        public DataStream datastream() { return this.datastream; }
        public BiFunction<String, Integer, Integer> partitioner() { return this.partitioner; }
    }

    public static StreamsParser parse(Pki pki) { return new StreamsParser(validate(pki)); }

    private static Pki validate(Pki pki) {
        if(pki == null) throw new IllegalArgumentException();

        return pki;
    }

    private final Pki pki;
    private StreamsParser(Pki pki) { this.pki = pki; }

    private static BiFunction<String, Integer, Integer> propsToPartitioner(Properties props) {
        switch (props.getProperty("partitioner")) {
            case "default-partitioner" :
                return (key, numPartitions) -> Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;

            case "topic-level-partitioner":
                int depth = Integer.parseInt(props.getProperty("partitioningDepth"));
                return (key, numPartitions) -> Utils.toPositive(Utils.murmur2(StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, key, depth).getBytes(StandardCharsets.UTF_8))) % numPartitions;

            default:
                throw new IllegalArgumentException("invalid partitioning schema");
        }
    }

    public StreamProps findStream(DataStream ds) {
        throw new NotImplementedException();
    }
}
