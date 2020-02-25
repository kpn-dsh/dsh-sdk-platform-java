package dsh.sdk;

import dsh.internal.AppId;
import dsh.internal.SslUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.Properties;

/** */
public class Sdk {
    private static final Logger logger = LoggerFactory.getLogger(Sdk.class);

    private static final String DSH_SECRET_TOKEN        = "DSH_SECRET_TOKEN";
    private static final String DSH_CA_CERTIFICATE      = "DSH_CA_CERTIFICATE";
    private static final String KAFKA_CONFIG_HOST       = "KAFKA_CONFIG_HOST";
    private static final String MESOS_TASK_ID           = "MESOS_TASK_ID";
    private static final String MARATHON_APP_ID         = "MARATHON_APP_ID";
    private static final String DSH_CONTAINER_DNS_NAME  = "DSH_CONTAINER_DNS_NAME";

    //
    private static String getEnvOrThrow(String key) {
        return Optional.ofNullable(System.getenv(key)).orElseThrow(() -> new IllegalArgumentException(key + " not defined"));
    }

    /**
     *
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
         *
         * @return
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
         *
         * @param token
         * @return
         */
        public Builder withToken(String token) { this.token = token; return this; }

        /**
         *
         * @param ca
         * @return
         */
        public Builder withCA(Certificate ca) { this.caCert = ca; return this; }

        /**
         *
         * @param host
         * @return
         */
        public Builder withPkiHost(String host) { this.pkiHost = host; return this; }

        /**
         *
         * @param appId
         * @return
         */
        public Builder withAppId(AppId appId) { this.appId = appId; return this; }

        /**
         *
         * @param taskId
         * @return
         */
        public Builder withTaskId(String taskId) { this.taskId = taskId; return this; }

        /**
         *
         * @param dnsName
         * @return
         */
        public Builder withDnsName(String dnsName) { this.dnsName = dnsName; return this; }

        /**
         *
         * @param value
         * @return
         */
        public Builder withServesSslEndpoint(boolean value) { this.servesSslEndpoint = value; return this; }

        /**
         *
         * @param propsFile
         * @return
         * @throws IOException
         */
        public Builder fromPropertiesFile(File propsFile) throws IOException {
            Properties props = new Properties();
            try(FileInputStream is = new FileInputStream(propsFile)) {
                props.load(is);
            }

            return fromProperties(props);
        }

        /**
         *
         * @param props
         * @return
         */
        public Builder fromProperties(Properties props) {
            this.allProps = props;
            return this;
        }

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
         *
         * @return
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

    //
    private Sdk() { throw new AssertionError(); }

    @Override
    public String toString() {
        return String.format( "Sdk(pki: %s)", pki.toString());
    }

    //
    private final PkiProvider pki;

    /**
     *
     * @param pki
     */
    public Sdk(PkiProvider pki) {
        this.pki = pki;
        logger.info("new Pki object created - {}", this.toString());
    }

    /**
     *
     * @return
     */
    public PkiProvider getPki() { return pki; }

    /**
     *
     * @return
     */
    public Properties getProps() { return pki.getProperties(); }

    public AppId getApp() { return this.pki.getAppId(); }
}
