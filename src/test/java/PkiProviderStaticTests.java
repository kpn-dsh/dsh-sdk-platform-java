import dsh.messages.Envelope;
import dsh.sdk.PkiProviderStatic;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PkiProviderStaticTests {
    Properties props = init();
    private Properties init() {
        Properties props = new Properties();
        props.put("consumerGroups.private", "tenant_application.00000000-0000-0000-0000-000000000000_1,tenant_application.00000000-0000-0000-0000-000000000000_2,tenant_application.00000000-0000-0000-0000-000000000000_3");
                return props;
    }

    @Test
    public void AppNameExtractedFromPropsShouldWork() {
        PkiProviderStatic p = new PkiProviderStatic(props);
        assertEquals(
                "application",
                p.getAppId().name()
        );
    }

    @Test
    public void TaskIdExtractedFromPropsShouldWork() {
        PkiProviderStatic p = new PkiProviderStatic(props);
        assertEquals(
                "00000000-0000-0000-0000-000000000000",
                p.getTaskId()
        );
    }

    @Test
    public void TenantIdExtractedFromPropsShouldWork() {
        PkiProviderStatic p = new PkiProviderStatic(props);
        assertEquals(
                "tenant",
                p.getAppId().root()
        );
    }

    @Test
    public void IdentityExtractedFromPropsShouldWork() {
        PkiProviderStatic p = new PkiProviderStatic(props);
        assertEquals(
                Envelope.Identity.newBuilder().setTenant("tenant").setApplication("application").build(),
                p.getAppId().identity()
        );
    }
}
