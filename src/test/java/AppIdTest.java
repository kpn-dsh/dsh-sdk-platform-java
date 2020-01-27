import dsh.messages.common.Envelope;
import dsh.internal.AppId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

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
                Envelope.Identity.newBuilder().setTenant("root").setPublisher("myapp").build(),
                i.identity()
        );
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void createShouldFail_onMissingLeadingSlash() {
        exception.expect(IllegalArgumentException.class);
        AppId.from("root/x/y/z/myapp");
    }

    @Test
    public void createShouldFail_onMissingLevels() {
        exception.expect(IllegalArgumentException.class);
        AppId.from("/myapp");
    }

    @Test
    public void createShouldFail_onEmpty() {
        exception.expect(IllegalArgumentException.class);
        AppId.from("");
    }

    @Test
    public void createShouldFail_onNull() {
        exception.expect(IllegalArgumentException.class);
        AppId.from(null);
    }
}
