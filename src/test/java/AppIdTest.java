import dsh.messages.Envelope;
import dsh.sdk.internal.AppId;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class AppIdTest {

    @Test
    public void createShouldWork() {
        AppId.from("/root/x/y/z/myapp");
    }

    @Test
    public void toStringShouldWork() {
        String id = "/root/x/y/z/myapp";
        assertTrue(AppId.from(id).toString().contains(id));
    }

    @Test
    public void getPropertiesShouldWork() {
        AppId i = AppId.from("/root/sub1/sub2/myapp");
        assertEquals("root", i.root());
        assertEquals(Stream.of("root", "sub1", "sub2").collect(Collectors.toCollection(HashSet::new)), i.groups());
        assertEquals("myapp", i.name());

        assertEquals(
                Envelope.Identity.newBuilder().setTenant("root").setApplication("myapp").build(),
                i.identity()
        );
    }

    @Test
    public void createShouldWork_onMissingLeadingSlash() {
        AppId i = AppId.from("root/sub1/sub2/myapp");
        assertEquals("root", i.root());
        assertEquals(Stream.of("root", "sub1", "sub2").collect(Collectors.toCollection(HashSet::new)), i.groups());
        assertEquals("myapp", i.name());

        assertEquals(
                Envelope.Identity.newBuilder().setTenant("root").setApplication("myapp").build(),
                i.identity()
        );
    }

    @Test
    public void createShouldFail_onMissingLevels() {
        assertThrows(IllegalArgumentException.class, () -> AppId.from("/myapp"));
    }

    @Test
    public void createShouldFail_onEmpty() {
        assertThrows(IllegalArgumentException.class, () -> AppId.from(""));
    }

    @Test
    public void createShouldFail_onNull() {
        assertThrows(IllegalArgumentException.class, () -> AppId.from(null));
    }
}
