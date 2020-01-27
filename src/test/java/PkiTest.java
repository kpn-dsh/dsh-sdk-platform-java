import dsh.kafka.KafkaParser;
import dsh.pki.Pki;
import dsh.streams.StreamsParser;
import mocks.PkiServerMock;
import mocks.SslMock;
import mocks.StreamMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.cert.Certificate;

import static org.junit.Assert.assertEquals;

public class PkiTest {
    private static Certificate TEST_CA;
    private static String KAFKA_PROPS;
    private static PkiServerMock pki;

    @BeforeClass
    public static void beforeAll() {
        SslMock.init();

        TEST_CA = SslMock.createCertificate("mytenant");
        KAFKA_PROPS = StreamMock.dataStreamProps("mytenant");
        pki = new PkiServerMock(6666, TEST_CA, KAFKA_PROPS);
        pki.start();
    }

    @AfterClass
    public static void afterAll() { pki.stop(); }

    private StreamsParser sparser;
    private KafkaParser kparser;

    @Test
    public void createPkiShouldWork() {
        pki.setTenant("mytenant");

        Pki undertest = new Pki(
                pki.connectionString(),
                "/mytenant/subgroup/myapp",
                "mytenant_subgroup_myapp.00000000-0000-0000-0000-000000000000",
                "****************",
                TEST_CA,
                "password.1",
                "myapp.subgroup.mytenant.localhost.net"
        );

        undertest.init(false);

        sparser = StreamsParser.parse(undertest);
        kparser = KafkaParser.parse(undertest);
    }

    @Test
    public void parsingStreamsInfoShouldWork() throws Exception {
        //TODO
    }

    @Test
    public void parsingKafkaPropsShouldWork() throws Exception {
        /* depends on */ createPkiShouldWork();

        assertEquals(
                "group_1",
                kparser.takeConsumerGroup(KafkaParser.ConsumerGroupType.SHARED)
        );
    }
}
