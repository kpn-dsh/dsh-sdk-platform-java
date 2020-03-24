package dsh.rest;

import com.sun.net.httpserver.HttpServer;
import dsh.sdk.internal.HttpUtils;
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
 * A convenience class to easily create a HTTP REST service from within your application.
 *
 * This REST service is tuned for usage as metrics scraper and health reporter.
 * In order to fulfill the requirements of returning a  body in the response, it is possible to hook up
 * a function to the service to supply a {@code Boolean} answer (used for health reporting),
 * or an answer with a {@code String} body (use for metric reporting).
 *
 * By default the service can handle functions (@see Supplier) that return a {@code String} or a {@code Boolean}.
 *
 * <pre>{@code
 *
 *  boolean isHealth()      { ... }
 *  String  scrapeMetrics() { ... }
 *
 *  Service myServer = new SimpleRestServer.Builder()
 *                          .setListener("/health", () -> this.isHealthy())
 *                          .setListener("/metrics", () -> this.scrapeMetrics())
 *
 *  myServer.start();
 *
 * }</pre>
 */
public class SimpleRestServer extends Service {
    private static Logger logger = LoggerFactory.getLogger(SimpleRestServer.class);

    /**
     * Builder class to configure thr REST Server
     */
    public static class Builder extends Service.Builder{
        private Integer port;
        private Map<String, Supplier<?>> services = new HashMap<>();

        /**
         * Assign a specific port the server will listen on.
         *
         * @param port port to listen on
         * @return Service Builder
         */
        public Builder setPort(Integer port) {
            this.port = port;
            return this;
        }

        /**
         * Assign a result supplier for a given path for the HTTP Service.
         *
         * @param path      the path for which this supplier will provide the result
         * @param provider  the provider to calculate the result
         * @param <T>       result type for the result supplier
         *                  <b>Boolean</b> for 200OK, 500ServiceUnavailable results
         *                  <b>String</b> for 200OK with result body
         * @return Service Builder
         */
        public <T> Builder setListener(String path, Supplier<T> provider) {
            services.putIfAbsent(path, provider);
            return this;
        }

        /**
         * Creates the HTTP REST server with the specified configuration options.
         *
         * @return a server that can be started and stopped
         * @exception ServiceConfigException when no available free listening ports can be found
         */
        @Override public Service build() throws ServiceConfigException {
            SimpleRestServer rest = new SimpleRestServer();
            try {
                rest.server = HttpServer.create(new InetSocketAddress(this.port == null ? HttpUtils.freePort() : this.port), 0);
                rest.server.setExecutor(Executors.newSingleThreadExecutor());
            }
            catch (IOException e) {
                throw new Service.ServiceConfigException(e);
            }

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

    private SimpleRestServer() { /* only accessible from the Builder */ }
    private HttpServer server;

    /**
     * Start the REST server
     */
    @Override public void start() { server.start(); }

    /**
     * Stop the REST server
     *
     * <b>note:</b> this function call can block for several seconds.
     */
    @Override public void stop() { server.stop(5); }

    /**
     * When used as a {@code Runnable} this will automatically start the server on calling {@link Runnable#run() run}.
     * @see #start()
     */
    @Override public void run() { this.start(); }

    /**
     * When used as a {@code Closeable} this will automatically stop the server on calling {@link Closeable#close() close}.
     * @see #stop()
     */
    @Override public void close() { this.stop(); }

    /**
     * Get the address the service is listening on
     * @return the socket the server is listening on
     * @see InetSocketAddress
     */
    @Override public InetSocketAddress address() { return server.getAddress(); }
}
