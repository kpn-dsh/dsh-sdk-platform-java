package dsh.rest;

import com.sun.net.httpserver.HttpServer;
import dsh.internal.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 *
 */
public class SimpleRestServer implements Runnable, Closeable {
    private static Logger logger = LoggerFactory.getLogger(SimpleRestServer.class);

    /**
     *
     */
    public static class Builder {
        private Integer port;
        private Map<String, Supplier<?>> services = new HashMap<>();

        /**
         *
         * @param port
         * @return
         */
        public Builder withPort(Integer port) {
            this.port = port;
            return this;
        }

        /**
         *
         * @param path
         * @param provider
         * @param <T>
         * @return
         */
        public <T> Builder withListener(String path, Supplier<T> provider) {
            services.putIfAbsent(path, provider);
            return this;
        }

        /**
         *
         * @return
         * @throws IOException
         */
        public SimpleRestServer build() throws IOException {
            SimpleRestServer rest = new SimpleRestServer();
            rest.server = HttpServer.create(new InetSocketAddress(this.port == null ? HttpUtils.freePort() : this.port), 0);
            rest.server.setExecutor(Executors.newSingleThreadExecutor());

            logger.info("build REST server @ {}, listeners: {}", this.port, String.join(",", this.services.keySet()));

            services.forEach((path, supp) ->
                    rest.server.createContext(path, http -> {
                        Consumer<Object> createResponse = (Object o) -> {
                            logger.debug("incoming rest request - path: {}, origin: {}", http.getRequestURI().getPath(), http.getRemoteAddress().toString());

                            try {
                                if (o == null) {
                                    http.sendResponseHeaders(404, -1);
                                } else if (o instanceof Boolean && !(Boolean) o) {
                                    http.sendResponseHeaders(500, -1);
                                } else if (o instanceof Boolean && (Boolean) o) {
                                    http.sendResponseHeaders(200, -1);
                                } else if (o instanceof String && ((String) o).isEmpty()) {
                                    logger.debug("format 'text/plain' EMPTY response body");
                                    http.getResponseHeaders().set("Content-Type", "text/plain, charset=UTF-8");
                                    http.sendResponseHeaders(200, -1);
                                } else if (o instanceof String) {
                                    try {
                                        logger.debug("format 'text/plain' response body - length:{}", ((String) o).length());

                                        http.getResponseHeaders().set("Content-Type", "text/plain, charset=UTF-8");
                                        http.sendResponseHeaders(200, ((String) o).getBytes(StandardCharsets.UTF_8).length);

                                        OutputStream os = http.getResponseBody();
                                        os.write(((String) o).getBytes(StandardCharsets.UTF_8));
                                        os.close();
                                    } catch (Exception e) {
                                        logger.debug("error processing http request - {} - path: {}, origin: {}",  e.getMessage(), http.getRequestURI().getPath(), http.getRemoteAddress().toString(), e);
                                        http.sendResponseHeaders(500, -1);
                                    }
                                }
                                else {
                                    logger.error("unable to create response for {}", o.getClass().getName());
                                    http.sendResponseHeaders(500, -1);
                                }
                            } catch (IOException e) {
                                logger.error("error processing http request - {}", e.getMessage(), e);
                            }
                        };

                        try { createResponse.accept(supp.get()); } finally { http.close(); }
                    })
            );

            return rest;
        }
    }

    public static Builder builder() { return new Builder(); }

    private SimpleRestServer() { /* only accessible from the Builder */ }
    private HttpServer server;

    /**
     *
     */
    public void start() { server.start(); }

    /**
     *
     */
    public void stop() { server.stop(5); }

    @Override public void run() { this.start(); }
    @Override public void close() { this.stop(); }

    /**
     *
     * @return
     */
    public InetSocketAddress address() { return server.getAddress(); }
}
