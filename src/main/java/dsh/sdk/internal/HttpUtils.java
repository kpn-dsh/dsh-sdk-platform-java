package dsh.sdk.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URLConnection;
import java.util.stream.Collectors;

/**
 * General purpose HTTP helper functions
 */
public class HttpUtils {

    /**
     * Read data (HTTP) from a socket.
     *
     * @param conn socket serving the HTTP request/response
     * @return data on the socket parsed as a character stream (String)
     * @throws IOException when the stream could not be fetched from the {@link URLConnection socket}
     */
    public static String contentOf(URLConnection conn) throws IOException {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    /**
     * Find a free port to use for a server to listen on.
     *
     * @return a free port
     * @throws IOException when no free port could be found
     */
    public static Integer freePort() throws IOException {
        try(ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
