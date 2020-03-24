import dsh.messages.Envelope;
import dsh.sdk.PkiProviderPikachu;
import dsh.sdk.PkiProviderStatic;
import dsh.sdk.Sdk;
import dsh.sdk.internal.AppId;
import dsh.sdk.internal.HttpUtils;
import dsh.sdk.kafka.KafkaConfigParser;
import mocks.PkiServerMock;
import mocks.SslMock;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SdkTest {
    private static Certificate TEST_CA;
    private static String KAFKA_PROPS;
    private static PkiServerMock pki;

    private static String dataStreamProps(String tenant) {
        final String APP = "xxx_application";
        final String UUID = "00000000-0000-0000-0000-000000000000";

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
        props.put("datastream.stream.ping.read", "stream\\.ping\\.[^.]*");
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

        try(StringWriter writer = new StringWriter()) {
            props.store(new PrintWriter(writer), null);
            return writer.getBuffer().toString();
        }
        catch (IOException e) { return null; }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        SslMock.init();

        TEST_CA = SslMock.createCertificate("mytenant");
        KAFKA_PROPS = dataStreamProps("mytenant");
        pki = new PkiServerMock(HttpUtils.freePort(), TEST_CA, KAFKA_PROPS);
        pki.start();
    }

    @AfterAll
    public static void afterAll() { pki.stop(); }

    @Test
    public void createWithPkiShouldWork() {
        pki.setTenant("mytenant");
        Sdk sdk = new Sdk(new PkiProviderPikachu(
                pki.connectionString(),
                TEST_CA,
                "****************",
                AppId.from("/mytenant/subgroup/myapp"),
                "mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000",
                "myapp.subgroup.mytenant.localhost.net",
                false
        ));

        assertEquals("myapp", sdk.getApp().name());
        assertEquals("mytenant", sdk.getApp().root());
        assertEquals(Envelope.Identity.newBuilder().setApplication("myapp").setTenant("mytenant").build(), sdk.getApp().identity());
        assertTrue(Arrays.asList("mytenant", "subgroup").containsAll(sdk.getApp().groups()));
    }

    private static Properties platformProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker-0.kafka.marathon.mesos:9091,broker-1.kafka.marathon.mesos:9091,broker-2.kafka.marathon.mesos:9091");
        props.put("consumerGroups.shared", IntStream.of(1,2,3).mapToObj(n -> "mytenant_subgroup_myapp_" + n).collect(Collectors.joining(",")));
        props.put("consumerGroups.private", IntStream.of(1,2,3).mapToObj(n -> "mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000_" + n).collect(Collectors.joining(",")));
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "truststore.jks");
        props.put("ssl.truststore.password", "truststore.password");
        props.put("ssl.keystore.location", "keystore.jks");
        props.put("ssl.keystore.password", "truststore.password");
        props.put("ssl.key.password", "key.password");

        return props;
    }

    @Test
    public void createStaticShouldWork() {
        Sdk sdk = new Sdk(new PkiProviderStatic(platformProps()));
        assertEquals("myapp", sdk.getApp().name());
        assertEquals("mytenant", sdk.getApp().root());
        assertEquals(Envelope.Identity.newBuilder().setApplication("myapp").setTenant("mytenant").build(), sdk.getApp().identity());
        assertTrue(Arrays.asList("mytenant", "subgroup").containsAll(sdk.getApp().groups()));
    }

    @Test
    public void kafkaParserCanBeGeneratedFromSdk() {
        KafkaConfigParser parser = KafkaConfigParser.of(new Sdk(new PkiProviderStatic(platformProps())));
        assertEquals("SSL", parser.kafkaProducerProperties(null).get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("SSL", parser.kafkaConsumerProperties(null).get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }
}
