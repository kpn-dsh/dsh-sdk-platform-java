package mocks;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

public class StreamMock {

    public static String dataStreamProps(String tenant) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker-0.kafka.marathon.mesos:9091,broker-1.kafka.marathon.mesos:9091,broker-2.kafka.marathon.mesos:9091");
        props.put("consumerGroups.shared", "group_1,group_2,group_3");
        props.put("consumerGroups.private", "group_private_1,group_private_2,group_private_3");
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
            props.list(new PrintWriter(writer));
            return writer.getBuffer().toString();
        }
        catch (IOException e) { return null; }
    }

}
