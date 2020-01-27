import dsh.messages.DataStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** */
public class DataStreamTests {

    @Test
    public void extractStreamFromTopicShouldReturnValidObject() {
        List<String> x = Arrays.stream(DataStream.StreamType.values()).map(DataStream.StreamType::name).collect(Collectors.toList());
        DataStream ds1 = new DataStream(DataStream.StreamType.INTERNAL, "my-stream");
        DataStream ds2 = DataStream.parse("internal.my-stream.xxx");

        assertEquals(new DataStream(DataStream.StreamType.INTERNAL, "my-stream"), DataStream.parse("internal.my-stream.xxx"));
        assertEquals(new DataStream(DataStream.StreamType.PUBLIC, "my-stream"), DataStream.parse("stream:my-stream"));
        assertEquals(new DataStream(DataStream.StreamType.SCRATCH, "my-stream"), DataStream.parse("scratch.my-stream"));
    }

    @Test
    public void extractStreamFromTopicShouldReturnNull() {
        assertNull(DataStream.parse(null));
    }
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void extractStreamFromTopicShouldThrow() {
        exception.expect(IllegalArgumentException.class);

        DataStream.parse("");
        assertNull(DataStream.parse(""));
        assertNull(DataStream.parse("internal"));
        assertNull(DataStream.parse("internal:"));
        assertNull(DataStream.parse("internal."));
        assertNull(DataStream.parse("internal:   "));
        assertNull(DataStream.parse("internal.   "));
        assertNull(DataStream.parse("internal: : "));
        assertNull(DataStream.parse("internal. . "));
    }
}
