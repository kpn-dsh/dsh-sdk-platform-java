import dsh.internal.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class StringTest {

    @Test
    public void takeLevelShouldGiveCorrectSubstringWhenLongerThanLevel() {
        assertEquals(
                "012",
                StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, "0/1/2/3/4/5/6/7/8/9", 3)
        );
    }

    @Test
    public void takeLevelShouldGiveCorrectSubstringWhenShorterThanLevel() {
        assertEquals(
                "0123456789",
                StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, "0/1/2/3/4/5/6/7/8/9", 16)
        );
    }

    @Test
    public void takeLevelShouldGiveCorrectSubstringWhenEqualToLevel() {
        assertEquals(
                "0123456789",
                StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, "0/1/2/3/4/5/6/7/8/9", 10)
        );
    }

    @Test
    public void takeLevelShouldWorkOnEmptyString() {
        assertEquals(
                "",
                StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, "", 10)
        );
    }

    @Test
    public void takeLevelShouldWorkOnNull() {
        assertNull(
                StringUtils.takeFlat(StringUtils.TOPIC_DELIMITER, null, 10)
        );
    }
}
