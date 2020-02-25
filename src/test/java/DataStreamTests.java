import dsh.messages.DataStream;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** */
public class DataStreamTests {

    @Test
    public void extractStreamFromTopicShouldReturnValidObject() {
        List<String> x = Arrays.stream(DataStream.StreamType.values()).map(DataStream.StreamType::name).collect(Collectors.toList());
        DataStream ds1 = new DataStream(DataStream.StreamType.INTERNAL, "my-stream");
        DataStream ds2 = DataStream.of("internal.my-stream.xxx");

        assertEquals(new DataStream(DataStream.StreamType.INTERNAL, "my-stream"), DataStream.of("internal.my-stream.xxx"));
        assertEquals(new DataStream(DataStream.StreamType.PUBLIC, "my-stream"), DataStream.of("stream:my-stream"));
        assertEquals(new DataStream(DataStream.StreamType.SCRATCH, "my-stream"), DataStream.of("scratch.my-stream"));
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
