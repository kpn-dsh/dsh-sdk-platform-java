package dsh.sdk;

import dsh.internal.AppId;

import java.io.File;
import java.util.Properties;

public interface PkiProvider {

    /**
     *
     */
    class PkiException extends RuntimeException {
        private static final long serialVersionUID = -7407930104204081137L;

        PkiException (String msg, Exception cause) { super(msg, cause); }
        PkiException (String msg) { super(msg); }
        PkiException (Exception cause) { super(cause); }
    }

    /**
     *
     * @return
     */
    AppId getAppId();

    /**
     *
     * @return
     */
    String getTaskId();

    /**
     *
     * @return
     */
    String getPassword();

    /**
     *
     * @return
     * @throws PkiException
     */
    File getKeystoreFile() throws PkiException;

    /**
     *
     * @return
     * @throws PkiException
     */
    File getTruststoreFile() throws PkiException;

    /**
     *
     * @return
     * @throws PkiException
     */
    Properties getProperties() throws PkiException;
}
