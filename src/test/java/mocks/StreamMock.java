package mocks;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamMock {

    public final static String APP = "xxx_application";
    public final static String UUID = "00000000-0000-0000-0000-000000000000";

    public static String dataStreamProps(String tenant) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker-0.kafka.marathon.mesos:9091,broker-1.kafka.marathon.mesos:9091,broker-2.kafka.marathon.mesos:9091");
        props.put("consumerGroups.shared", IntStream.of(1,2,3).mapToObj(n -> APP + "_" + n).collect(Collectors.joining(",")));
        props.put("consumerGroups.private", IntStream.of(1,2,3).mapToObj(n -> APP + "." + UUID + "_" + n).collect(Collectors.joining(",")));
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "truststore.jks");
        props.put("ssl.truststore.password", "truststore.password");
        props.put("ssl.keystore.location", "keystore.jks");
        props.put("ssl.keystore.password", "truststore.password");
        props.put("ssl.key.password", "key.password");

        props.put("datastream.stream.ping.partitions", "12");
        props.put("datastream.stream.ping.partitioner", "topic-level-partitioner");
        props.put("datastream.stream.ping.replication", "3");
        props.put("datastream.stream.ping.read", "stream\\\\.ping\\\\.[^.]*");
        props.put("datastream.stream.ping.canretain", "true");
        props.put("datastream.stream.ping.cluster", "/tt");
        props.put("datastream.stream.ping.partitioningDepth", "1");

        props.put("datastream.internal.testprops.partitions", "1");
        props.put("datastream.internal.testprops.partitioner", "default-partitioner");
        props.put("datastream.internal.testprops.replication", "3");
        props.put("datastream.internal.testprops.read", "internal.testprops."+ tenant);
        props.put("datastream.internal.testprops.canretain", "false");
        props.put("datastream.internal.testprops.cluster", "/tt");
        props.put("datastream.internal.testprops.partitioningDepth", "0");

        props.put("datastream.scratch.black-mesa.partitions", "6");
        props.put("datastream.scratch.black-mesa.partitioner", "default-partitioner");
        props.put("datastream.scratch.black-mesa.replication", "1");
        props.put("datastream.scratch.black-mesa.read", "scratch.black-mesa."+ tenant);
        props.put("datastream.scratch.black-mesa.write", "scratch.black-mesa."+ tenant);
        props.put("datastream.scratch.black-mesa.canretain", "false");
        props.put("datastream.scratch.black-mesa.cluster", "/tt");
        props.put("datastream.scratch.black-mesa.partitioningDepth", "0");

        try(StringWriter writer = new StringWriter()) {
            props.store(new PrintWriter(writer), null);
            return writer.getBuffer().toString();
        }
        catch (IOException e) { return null; }
    }

}
