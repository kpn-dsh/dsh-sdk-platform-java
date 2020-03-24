import dsh.messages.DataStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** */
public class DataStreamTests {

    @Test
    public void extractStreamFromTopicShouldReturnValidObject() {
        assertEquals(DataStream.of(DataStream.StreamType.INTERNAL, "my-stream"), DataStream.of("internal.my-stream.xxx"));
        assertEquals(DataStream.of(DataStream.StreamType.PUBLIC, "my-stream"), DataStream.of("stream:my-stream"));
    }

    @Test
    public void extractStreamFromTopicShouldReturnNull() {
        assertNull(DataStream.of(null));
    }

    @Test
    public void extractStreamFromTopicShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> DataStream.of(""));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal"));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal:"));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal."));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal:   "));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal.   "));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal: : "));
        assertThrows(IllegalArgumentException.class, () -> DataStream.of("internal. . "));
    }
}
