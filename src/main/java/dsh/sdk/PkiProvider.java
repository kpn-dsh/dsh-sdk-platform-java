package dsh.sdk;

import dsh.sdk.internal.AppId;

import java.io.File;
import java.util.Properties;

/**
 * Interface for usage inside the SDK
 * The class deriving from this interface implements the actions needed to get the required data
 * to feed to the individual parsers that are initialized from the main SDK object.
 *
 * It basically defines the internal API that is used to give the individial parsers access to the data they require.
 */
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
     * Application ID
     */
    AppId getAppId();

    /**
     * Task ID
     */
    String getTaskId();

    /**
     * Keystore/Truststore password
     */
    String getPassword();

    /**
     * The keystore
     * @throws PkiException
     */
    File getKeystoreFile() throws PkiException;

    /**
     * The truststore
     * @throws PkiException
     */
    File getTruststoreFile() throws PkiException;

    /**
     * full set of properties that can be used by any parser implementation
     * @throws PkiException
     */
    Properties getProperties() throws PkiException;
}
