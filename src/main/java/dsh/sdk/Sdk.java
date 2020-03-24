package dsh.sdk;

import dsh.sdk.internal.AppId;
import dsh.sdk.internal.SslUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.Properties;

/**
 * The main class for the platform Java SDK -- sal starts from here.
 *
 * It's basically used to configure the SDK builder "how" the SDK should be initialized.
 * - from environment variables, including possible handshaking with the platform PKI service
 * - from existing properties
 * - auto detect (so basically meaning: one of the above)
 *
 * Initialize the SDK from the environment variables:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder().fromEnv().build();
 * }</pre>
 *
 * Automatically detect how the SDK should be initialized:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder().autoDetect().build();
 * }</pre>
 *
 * To manually build up the configuration, provide it in a pre-created properties file (or object)
 * or just call the individual builder functions to set/unset certain properties.
 *
 * Initialize from given properties file:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder().fromPropertiesFile(File).build();
 * }</pre>
 *
 *  Initialize from given properties object:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder().fromProperties(Properties).build();
 * }</pre>
 *
 * Manually initialize all properties from code:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder()
 *                  .setToken      (String)     / .resetToken()
 *                  .setCA         (String)     / .resetCA()
 *                  .setPkiHost    (String)     / .resetPkiHost()
 *                  .setAppId      (AppId)      / .resetAppId()
 *                  .setTaskId     (String)     / .resetTaskId()
 *                  .setDnsName    (String)     / .resetDnsName()
 *                  .setServesSslEndpoint ()    / .resetServesSslEndpoint()
 *                  .build();
 * }</pre>
 *
 * You can also combine the builder functions e.g. to override a single parameter:
 * <pre>{@code
 *     Sdk sdk = new Sdk.Builder().autoDetect()
 *                  .setAppId       (AppId)
 *                  .resetTaskId    (String)
 *                  .build();
 * }</pre>
 */
public class Sdk {
    private static final Logger logger = LoggerFactory.getLogger(Sdk.class);

    //TODO: make these configurable, together with a function "how" to extract the needed info from the variable (regex?)
    private static final String DSH_SECRET_TOKEN        = "DSH_SECRET_TOKEN";       // String -- mandatory (not empty)
    private static final String DSH_CA_CERTIFICATE      = "DSH_CA_CERTIFICATE";     // String -- mandatory (valid cert.)
    private static final String KAFKA_CONFIG_HOST       = "KAFKA_CONFIG_HOST";      // String -- optional (valid URL) -- mandatory for KafkaParser/StreamsParser
    private static final String MESOS_TASK_ID           = "MESOS_TASK_ID";          // String -- mandatory (regex)
    private static final String MARATHON_APP_ID         = "MARATHON_APP_ID";        // String -- mandatory (regex)
    private static final String DSH_CONTAINER_DNS_NAME  = "DSH_CONTAINER_DNS_NAME"; // String -- mandatory (???)

    // fetch the value of a system property -- throw IllegalArgument Exception when not found or empty
    private static String getEnvOrThrow(String key) {
        return Optional.ofNullable(System.getenv(key)).filter(s -> ! s.isEmpty()).orElseThrow(() -> new IllegalArgumentException(key + " not defined (or empty)"));
    }

    /**
     * Builder class for configuring the SDK
     */
    public static class Builder {
        private String token;
        private Certificate caCert;
        private String pkiHost;
        private String taskId;
        private AppId  appId;
        private String dnsName;
        private Properties allProps;
        private boolean servesSslEndpoint;

        /**
         * initialize the SDK from the provided environment variables.
         * Whenever certain required environment variables are not found, an [[IllegalArgumentException]] will be thrown
         * @return SDK Builder
         * @exception IllegalArgumentException on certificate error
         * @exception IllegalArgumentException on invalid or empty environment variables
         */
        public Builder fromEnv() {
            try {
                token = getEnvOrThrow(DSH_SECRET_TOKEN);
                caCert = SslUtils.certificateFromPEM(getEnvOrThrow(DSH_CA_CERTIFICATE));
                pkiHost = getEnvOrThrow(KAFKA_CONFIG_HOST);
                taskId = getEnvOrThrow(MESOS_TASK_ID);
                appId = AppId.from(getEnvOrThrow(MARATHON_APP_ID));
                dnsName = getEnvOrThrow(DSH_CONTAINER_DNS_NAME);

                return this;
            }
            catch (CertificateException e) {
                throw new IllegalArgumentException("Invalid root certificate");
            }
        }

        /**
         * Sets the specified String as {@code token} to configure the SDK with.
         * @param token  the token (as String) how it was presented by the platform
         * @return SDK Builder
         */
        public Builder setToken(String token) { this.token = token; return this; }

        /**
         * Removes whatever token got configured in the {@code Builder}.
         * @return SDK Builder
         */
        public Builder resetToken() { this.token = null; return this; }

        /**
         * Configure the root CA used to handshake with platform
         * @param ca the root CA used for the platform
         * @return SDK Builder
         */
        public Builder setCA(Certificate ca) { this.caCert = ca; return this; }

        /**
         * Removes whatever root CA got configured in the {@code Builder}.
         * @return SDK Builder
         */
        public Builder resetCA() { this.caCert = null; return this; }

        /**
         * Configure how to connect to the platform PKI service
         * @param host Connection string for the platforms PKI service
         * @return SDK Builder
         */
        public Builder setPkiHost(String host) { this.pkiHost = host; return this; }

        /**
         * Removes whatever PKI Host got configured in the {@code Builder}.
         * @return SDK Builder
         */
        public Builder resetPkiHost() { this.pkiHost = null; return this; }

        /**
         * Sets the application identifier
         * @param appId this applications identifier
         * @return SDK Builder
         */
        public Builder setAppId(AppId appId) { this.appId = appId; return this; }

        /**
         * Removes whatever AppId got configured in the {@code Builder}.
         * @return SDK Builder
         */
        public Builder resetAppId() { this.appId = null; return this; }

        /**
         *
         * @param taskId
         * @return SDK Builder
         */
        public Builder setTaskId(String taskId) { this.taskId = taskId; return this; }

        /**
         * Removes whatever TaskId got configured in the {@code Builder}.
         * @return SDK Builder
         */
        public Builder resetTaskId() { this.taskId = null; return this; }

        /**
         * Set the DNS name for this application.
         * This dns name will also be added to the certificate signing request when handshaking with
         * the platforms PKI service would be done.
         *
         * This setting is only applicable when the application also serves an SSL endpoint (@see setServesSslEndpoint)
         *
         * @param dnsName
         * @return SDK Builder
         */
        public Builder setDnsName(String dnsName) { this.dnsName = dnsName; return this; }

        /**
         * Remove whatever DNS name got configured in the {@code Builder}
         * @return SDK Builder
         */
        public Builder resetDnsName() { this.dnsName = null; return this; }

        /**
         * Mark this application as a service that will serve TLS connections
         * (@see setDnsName)
         *
         * @return SDK Builder
         */
        public Builder setServesSslEndpoint() { this.servesSslEndpoint = true; return this; }

        /**
         * Mark this application as a service that won't serve any TLS connections.
         * (@see setDnsName)
         *
         * @return SDK Builder
         */
        public Builder resetServesSslEndpoint() { this.servesSslEndpoint = false; return this; }

        /**
         * Initialize the SDK from an existing properties file.
         * This will prevent any handshaking with the platforms PKI service as it will be assumed
         * that all required configuration is present in the properties file.
         *
         * @param propsFile  the {@code File} containing all configuration properties.
         * @return SDK Builder
         * @throws IOException when an error occures accessing the given file.
         */
        public Builder fromPropertiesFile(File propsFile) throws IOException {
            Properties props = new Properties();
            try(FileInputStream is = new FileInputStream(propsFile)) {
                props.load(is);
            }

            return fromProperties(props);
        }

        /**
         * Initialize the SDK from an existing properties object.
         * This will prevent any handshaking with the platforms PKI service as it will be assumed
         * that all required configuration is present in the given {@code properties} object.
         *
         * @param props  the {@code Properties} containing all configuration settings.
         * @return SDK Builder
         */
        public Builder fromProperties(Properties props) {
            this.allProps = props;
            return this;
        }

        /**
         * Automatically try to detect how to initialize the SDK.
         * When the system property {@code platform.properties.file} is present, the SDK will be initialized from this file,
         * assuming all required configuration is in there.  When something goes wrong when trying to initialize the SDK from
         * this file, or the {@code platform.properties.file} was not set, a fallback will be done to initialization from the environment variables.
         *
         * @return SDK Builder
         * @exception IllegalArgumentException on certificate error
         * @exception IllegalArgumentException on invalid or empty environment variables
         * @see #fromEnv() fromEnv
         * @see #fromPropertiesFile(File)  fromPropertiesFile
         */
        public Builder autoDetect() {
            if (System.getProperties().containsKey("platform.properties.file")) {
                try {
                    return fromPropertiesFile(new File(System.getProperties().getProperty("platform.properties.file")));
                }
                catch (IOException e) {
                    logger.error("error reading platform properties (fallback to environment loading) - {}", e.getMessage(), e);
                    return fromEnv();
                }
            }
            else
                return fromEnv();
        }

        /**
         * Validate and apply the SDK configuration
         * @return a ready-to-use {@link Sdk} object.
         */
        public Sdk build() {
            try {
                if(allProps != null && ! allProps.isEmpty()) {
                    return new Sdk(new PkiProviderStatic(allProps));
                }
                else {
                    return new Sdk(new PkiProviderPikachu(
                            pkiHost, caCert, token, appId, taskId, dnsName, servesSslEndpoint
                    ));
                }
            }
            catch(Exception e) {
                throw new IllegalArgumentException("Unable to create PKI parser", e);
            }
        }
    }

    // default constructor can not be used
    private Sdk() { throw new AssertionError(); }

    @Override
    public String toString() {
        return String.format( "Sdk(pki: %s)", pki.toString());
    }

    // the provider of the PKI properties
    //  can be communication with the platform PKI service or static properties
    private final PkiProvider pki;

    /**
     * Create a new SDK object with the given provider
     * @param pki the PKI provider
     * @see PkiProvider PkiProvider
     */
    public Sdk(PkiProvider pki) {
        this.pki = pki;
        logger.info("new Pki object created - {}", this.toString());
    }

    // only for internal use to get access to the PKI configuration:
    //  truststore, keystore, password, ...
    public PkiProvider getPki() { return pki; }

    /**
     * Fetch all properties.
     * Properties can be queried on-the-fly from the platforms PKI service,
     * or where provided statically, depending on the way the SDK got initialized.
     *
     * @return all properties
     */
    public Properties getProps() { return pki.getProperties(); }

    /**
     * Gets the application identity to use.
     * @return the application identity
     * @see AppId
     */
    public AppId getApp() { return this.pki.getAppId(); }
}
