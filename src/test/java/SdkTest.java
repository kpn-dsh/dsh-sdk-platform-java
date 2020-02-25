import dsh.internal.AppId;
import dsh.kafka.KafkaParser;
import dsh.sdk.PkiProviderPikachu;
import dsh.sdk.Sdk;
import dsh.streams.StreamsParser;
import mocks.PkiServerMock;
import mocks.SslMock;
import mocks.StreamMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SdkTest {
    private static Certificate TEST_CA;
    private static String KAFKA_PROPS;
    private static PkiServerMock pki;

    @BeforeAll
    public static void beforeAll() {
        SslMock.init();

        TEST_CA = SslMock.createCertificate("mytenant");
        KAFKA_PROPS = StreamMock.dataStreamProps("mytenant");
        pki = new PkiServerMock(6666, TEST_CA, KAFKA_PROPS);
        pki.start();
    }

    @AfterAll
    public static void afterAll() { pki.stop(); }

    private StreamsParser sparser;
    private KafkaParser kparser;

    @Test
    public void createPkiShouldWork() {
        pki.setTenant("mytenant");

        Sdk undertest = new Sdk(new PkiProviderPikachu(
                pki.connectionString(),
                TEST_CA,
                "****************",
                AppId.from("/mytenant/subgroup/myapp"),
                "mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000",
                "myapp.subgroup.mytenant.localhost.net",
                false
        ));

        sparser = StreamsParser.of(undertest);
        kparser = KafkaParser.of(undertest);
    }

    @Test
    public void parsingStreamsInfoShouldWork() {
        //TODO
    }

    @Test
    public void parsingKafkaPropsShouldWork() {
        /* depends on */ createPkiShouldWork();

        assertEquals(
                StreamMock.APP + "_1",
                kparser.suggestedConsumerGroup(KafkaParser.ConsumerGroupType.SHARED)
        );

        assertEquals(
                StreamMock.APP + "." + StreamMock.UUID + "_1",
                kparser.suggestedConsumerGroup(KafkaParser.ConsumerGroupType.PRIVATE)
        );
    }
}
