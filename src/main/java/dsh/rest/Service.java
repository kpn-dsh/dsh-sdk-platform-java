package dsh.rest;

import java.io.Closeable;
import java.net.InetSocketAddress;

/**
 * The {@code Service} class represents an abstract "service".
 * <p>
 * This class basically provides a generic API that can be used for any abstract service.
 * A service in this case would be something that can be started and stopped, and has a Builder to configure it.
 * This abstract base class (API) is just here for convenience purposes and has no real functional
 */
public abstract class Service implements Runnable, Closeable {
    abstract public void start();
    abstract void stop();
    abstract public InetSocketAddress address();

    @Override
    public void run() { this.start(); }

    @Override
    public void close() { this.stop(); }

    public abstract static class Builder {
        abstract public Service build() throws ServiceConfigException;
    }

    /**
     * possible exceptions
     */
    public static class ServiceConfigException extends RuntimeException {
        private static final long serialVersionUID = -7407930104204081137L;

        public ServiceConfigException (String msg, Exception cause) { super(msg, cause); }
        public ServiceConfigException (String msg) { super(msg); }
        public ServiceConfigException (Exception cause) { super(cause); }
    }
}
