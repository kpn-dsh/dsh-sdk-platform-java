package dsh.messages;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** */
public class Utils {

    public static List<String> topicSplit(String source, char delim) {
        if(source == null) throw new IllegalArgumentException();

        int firstDot = source.indexOf(delim);
        if (firstDot == -1) throw new IllegalArgumentException();
        else {
            int lastDot = source.lastIndexOf(delim);
            if (lastDot != firstDot) return Arrays.asList(source.substring(0, firstDot), source.substring(firstDot + 1, lastDot), source.substring(lastDot + 1));
            else return Arrays.asList(source.substring(0, firstDot), source.substring(firstDot + 1), null);
        }
    }
}
