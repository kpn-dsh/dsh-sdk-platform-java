import dsh.messages.DataStream;
import dsh.sdk.streams.StreamsConfigParser;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class StreamsConfigParserTests {

    private static Map<String, ?> defaults = new HashMap<String, Object>() {{
        put("datastream.stream.ping.partitions", "10");
        put("datastream.stream.ping.partitioner", "topic-level-partitioner");
        put("datastream.stream.ping.replication", "3");
        put("datastream.stream.ping.read", "stream\\.ping\\.[^.]+");
        put("datastream.stream.ping.write", "stream.ping.xxx");
        put("datastream.stream.ping.canretain", "false");
        put("datastream.stream.ping.cluster", "/tt");
        put("datastream.stream.ping.partitioningDepth", "1");

        put("datastream.internal.pang.partitions", "60");
        put("datastream.internal.pang.partitioner", "topic-level-partitioner");
        put("datastream.internal.pang.replication", "2");
        put("datastream.internal.pang.read", "internal\\.pang\\.[^.]+");
        put("datastream.internal.pang.write", "internal.pang.xxx");
        put("datastream.internal.pang.canretain", "false");
        put("datastream.internal.pang.cluster", "/tt");
        put("datastream.internal.pang.partitioningDepth", "12");

        put("datastream.stream.pong.partitions", "1");
        put("datastream.stream.pong.partitioner", "default-partitioner");
        put("datastream.stream.pong.replication", "2");
        put("datastream.stream.pong.read", "stream\\.pong\\.[^.]+");
        put("datastream.stream.pong.write", "stream.pong.xxx");
        put("datastream.stream.pong.canretain", "true");
        put("datastream.stream.pong.cluster", "/tt");
    }};

    @Test
    public void parserCanBeInitializedFromEmptyProperties() {
        assertNotNull(StreamsConfigParser.of(new Properties()));
    }

    @Test
    public void parserCanBeInitializedFromEmptyMap() {
        assertNotNull(StreamsConfigParser.of(new HashMap<>()));
    }

    @Test
    public void parserCanBeInitializedFromMap() {
        assertNotNull(StreamsConfigParser.of(defaults));
    }

    @Test
    public void parserCanBeInitializedFromProperties() {
        Properties props = new Properties();
        props.putAll(defaults);

        assertNotNull(StreamsConfigParser.of(props));
    }

    @Test
    public void parserCanBePrinted() {
        StreamsConfigParser parser = StreamsConfigParser.of(defaults);
        assertTrue(parser.toString().contains("streamsparser"));
        assertTrue(parser.toString().contains(String.format("%x", parser.hashCode())));
    }

    @Test
    public void streamContractCanBePrinted() {
        StreamsConfigParser.StreamContract contract = StreamsConfigParser.of(defaults).findStream(DataStream.of("stream:ping")).get();
        assertTrue(contract.toString().contains("streamcontract"));
        assertTrue(contract.toString().contains(String.format("%x", contract.hashCode())));
    }

    @Test
    public void parserCanHandleInvalidConfig() {
        Properties props = new Properties();
        props.put("some.unsupported.property", "???");
        props.put("datastream.SOMETHINGUNSUPPORTED.xxx.yyy", "???");
        props.put("some.other.unsupported.property", "???");

        assertNotNull(StreamsConfigParser.of(props));
    }

    @Test
    public void parserGivesOutContract() {
        StreamsConfigParser parser = StreamsConfigParser.of(defaults);
        assertTrue(parser.findStream(DataStream.of("stream:ping")).filter(c -> c.produceTopic().isPresent()).isPresent());
        assertTrue(parser.findStream(DataStream.of("stream:ping")).filter(c -> c.subscribePattern().isPresent()).isPresent());
        assertTrue(parser.findStream(DataStream.of("stream:ping")).filter(c -> c.datastream().equals(DataStream.of("stream:ping"))).isPresent());
        assertTrue(parser.findStream(DataStream.of("stream:ping")).filter(c -> c.partitioner() != null).isPresent());
    }

    @Test
    public void parserGivesOutDefaultPartitionerWhenNotConfigured() {
        Properties props = new Properties();
        props.putAll(defaults);
        props.remove("datastream.stream.ping.partitioner");

        StreamsConfigParser parser = StreamsConfigParser.of(props);

        assertTrue(parser.findStream(DataStream.of("stream:ping")).isPresent());
        assertTrue(parser.findStream(DataStream.of("stream:ping")).filter(c -> c.partitioner() != null).isPresent());
    }

    @Test
    public void parserCanGenerateMultiStreamSubscriptionPattern() {
        Pattern pattern = StreamsConfigParser.of(defaults).subscriptionPatternFor(
                DataStream.of("stream:ping"), DataStream.of("internal:pang")
        );

        assertTrue(pattern.matcher("stream.ping.111").find());
        assertTrue(pattern.matcher("stream.ping.222").find());
        assertTrue(pattern.matcher("internal.pang.111").find());
        assertTrue(pattern.matcher("internal.pang.222").find());
    }

    @Test
    public void streamsCanBeSearchedFor() {
        StreamsConfigParser parser = StreamsConfigParser.of(defaults);

        List<StreamsConfigParser.StreamContract> r = Arrays.stream(new String[]{"stream:ping", "stream:unknown1"})
                .map(DataStream::of)
                .map(parser::findStream)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        assertEquals(1, r.size());
    }
}
