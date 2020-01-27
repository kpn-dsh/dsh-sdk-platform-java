package dsh.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URLConnection;
import java.util.stream.Collectors;

/**
 *
 */
public class HttpUtils {

    /**
     *
     * @param conn
     * @return
     * @throws IOException
     */
    public static String contentOf(URLConnection conn) throws IOException {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    public static Integer freePort() throws IOException {
        try(ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
