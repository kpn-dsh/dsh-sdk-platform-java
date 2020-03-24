import dsh.sdk.kafka.KafkaConfigParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 */
public class KafkaConfigParserTests {
    private static Map<String, ?> defaults = new HashMap<String, Object>() {{
        put("bootstrap.servers", "broker-0.kafka.marathon.mesos:9091,broker-1.kafka.marathon.mesos:9091,broker-2.kafka.marathon.mesos:9091");
        put("consumerGroups.shared", IntStream.of(1,2,3).mapToObj(n -> "mytenant_subgroup_myapp_" + n).collect(Collectors.joining(",")));
        put("consumerGroups.private", IntStream.of(1,2,3).mapToObj(n -> "mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000_" + n).collect(Collectors.joining(",")));
        put("security.protocol", "SSL");
        put("ssl.truststore.location", "truststore.jks");
        put("ssl.truststore.password", "truststore.password");
        put("ssl.keystore.location", "keystore.jks");
        put("ssl.keystore.password", "truststore.password");
        put("ssl.key.password", "key.password");
    }};

    @Test
    public void parserCanBeInitializedFromEmptyProperties() {
        assertNotNull(KafkaConfigParser.of(new Properties()));
    }

    @Test
    public void parserCanBeInitializedFromEmptyMap() {
        assertNotNull(KafkaConfigParser.of(new HashMap<>()));
    }

    @Test
    public void parserCanBeInitializedFromMap() {
        assertNotNull(KafkaConfigParser.of(defaults));
    }

    @Test
    public void parserCanBeInitializedFromProperties() {
        Properties props = new Properties();
        props.putAll(defaults);

        assertNotNull(KafkaConfigParser.of(props));
    }

    @Test
    public void parserGivesOutProducerProperties() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        Properties props = parser.kafkaProducerProperties(null);

        assertEquals(7, props.size());
        assertTrue(defaults.keySet().containsAll(props.keySet()));
    }

    @Test
    public void parserGivesOutConsumerProperties() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        Properties props = parser.kafkaConsumerProperties(null);

        assertEquals(7, props.size());
        assertTrue(defaults.keySet().containsAll(props.keySet()));
    }

    @Test
    public void consumerPropertiesCanBeExtended() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        Properties custom = new Properties();
        custom.put("a.b.c", "666");
        Properties props = parser.kafkaConsumerProperties(custom);

        assertEquals("666", props.get("a.b.c"));
    }

    @Test
    public void producerPropertiesCanBeExtended() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        Properties custom = new Properties();
        custom.put("a.b.c", "666");
        Properties props = parser.kafkaProducerProperties(custom);

        assertEquals("666", props.get("a.b.c"));
    }

    @Test
    public void parserSuggestsValidConsumergroup() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        assertEquals("mytenant_subgroup_myapp_1", parser.suggestedConsumerGroup(KafkaConfigParser.ConsumerGroupType.SHARED));
        assertEquals("mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000_1", parser.suggestedConsumerGroup(KafkaConfigParser.ConsumerGroupType.PRIVATE));
    }

    @Test
    public void parserCanAddCustomConsumergroup() {
        KafkaConfigParser parser = KafkaConfigParser.of(defaults);
        Properties custom = new Properties();
        custom.put("a.b.c", "666");

        Properties props = KafkaConfigParser.addConsumerGroup("my-consumergroup" ,parser.kafkaConsumerProperties(custom));
        assertEquals("my-consumergroup", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("666", props.get("a.b.c"));
    }
}
