package dsh.pki;

import dsh.internal.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.Properties;

/** */
public class Pki {
    private static final Logger logger = LoggerFactory.getLogger(Pki.class);

    private static final String DSH_SECRET_TOKEN        = "DSH_SECRET_TOKEN";
    private static final String DSH_CA_CERTIFICATE      = "DSH_CA_CERTIFICATE";
    private static final String KAFKA_CONFIG_HOST       = "KAFKA_CONFIG_HOST";
    private static final String MESOS_TASK_ID           = "MESOS_TASK_ID";
    private static final String MARATHON_APP_ID         = "MARATHON_APP_ID";
    private static final String DSH_CONTAINER_DNS_NAME  = "DSH_CONTAINER_DNS_NAME";

    private static String getEnvOrThrow(String key) {
        return Optional.ofNullable(System.getenv(key)).orElseThrow(() -> new IllegalArgumentException(key + " not defined"));
    }

    /**
     * Create a ne Pki object to handshake with the platform,
     * based on the environment variables passed by the platform to the application.
     *
     * @return Pki object for this application.
     */
    public static Pki fromEnv() {
        String token    = getEnvOrThrow(DSH_SECRET_TOKEN);
        String ca       = getEnvOrThrow(DSH_CA_CERTIFICATE);
        String pkiHost  = getEnvOrThrow(KAFKA_CONFIG_HOST);
        String taskId   = getEnvOrThrow(MESOS_TASK_ID);
        String appId    = getEnvOrThrow(MARATHON_APP_ID);
        String dnsName  = getEnvOrThrow(DSH_CONTAINER_DNS_NAME);

        try {
            return new Pki(
                    pkiHost,
                    appId,
                    taskId,
                    token,
                    SslUtils.certificateFromPEM(ca),
                    SslUtils.generatePassword(32),
                    dnsName
            );
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Unable to create PKI parser", e);
        }
    }

    private Pki() { throw new AssertionError(); }

    private final String pkiHost;
    private final AppId appId;
    private final String taskId;
    private final String token;
    private final String dnsName;
    private final String password;
    private final KeyPair key;
    private final KeyStore truststore;
    private final KeyStore keystore;

    /** */
    public Pki(String pkiHost, String appId, String taskId, String token, Certificate ca, String password, String dnsName) {
        this.pkiHost = pkiHost;
        this.taskId = taskId;
        this.dnsName = dnsName != null && !dnsName.isEmpty() ? dnsName : null;
        this.token = token;
        this.password = password;
        this.appId = AppId.from(appId);

        Security.addProvider(new BouncyCastleProvider());

        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", BouncyCastleProvider.PROVIDER_NAME);
            keyGen.initialize(2048, new SecureRandom());

            this.key = keyGen.generateKeyPair();
            this.keystore = SslUtils.createKeystore(password, ca);
            this.truststore = SslUtils.createKeystore(password, ca);
        }
        catch(KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException | NoSuchProviderException e) {
            throw new IllegalArgumentException("Invalid security parameters", e);
        }

        logger.info("new Pki object created - {}", this.toString());
    }

    /** */
    private String getDN() throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException, IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) PkiUtils.pkiUrlForPath(pkiHost, PkiUtils.pathForDN(appId.root(), taskId)).openConnection();
            logger.debug("issue http request - {}", conn.getURL().toString());

            if (conn instanceof HttpsURLConnection)
                ((HttpsURLConnection) conn).setSSLSocketFactory(SslUtils.getSocketFactory(truststore, keystore, password));

            return HttpUtils.contentOf(conn);
        }
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private Properties fetchKafkaProps() {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) PkiUtils.pkiUrlForPath(pkiHost, PkiUtils.pathForKafkaProps(appId.root(), taskId)).openConnection();
            logger.debug("issue http request - {}", conn.getURL().toString());

            if(conn instanceof HttpsURLConnection) ((HttpsURLConnection)conn).setSSLSocketFactory(SslUtils.getSocketFactory(truststore, keystore, password));
            conn.setRequestProperty("X-Kafka-Config-Token", token);
            return Conversion.convertToProperties(HttpUtils.contentOf(conn));
        }
        catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException | IOException e) {
            throw new IllegalArgumentException("Invalid security parameters", e);
        }
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private Certificate sign(PKCS10CertificationRequest csr) {
        HttpURLConnection conn = null;
        try {

            conn = (HttpURLConnection) PkiUtils.pkiUrlForPath(pkiHost, PkiUtils.pathForSigning(appId.root(), taskId)).openConnection();
            logger.debug("issue http request - {}", conn.getURL().toString());

            if(conn instanceof HttpsURLConnection) ((HttpsURLConnection)conn).setSSLSocketFactory(SslUtils.getSocketFactory(truststore, keystore, password));
            conn.setRequestProperty("X-Kafka-Config-Token", token);

            byte[] data = SslUtils.stringFromPEM(csr).getBytes(StandardCharsets.UTF_8);

            // POST
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("charset", "utf-8");
            conn.setRequestProperty("Content-Length", Integer.toString(data.length));
            conn.setUseCaches(false);

            try(DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
                out.write(data);
            }

           return SslUtils.certificateFromPEM(HttpUtils.contentOf(conn));
        }
        catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException | IOException | CertificateException e) {
            throw new IllegalArgumentException("Invalid security parameters", e);
        }
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private void fetchKafkaCert(String dn, Boolean servesSslEndpoint) throws KeyStoreException, OperatorCreationException, IOException {
        Certificate cert = sign(SslUtils.buildCsr(dn, key, servesSslEndpoint? dnsName : null));
        keystore.setCertificateEntry("server", cert);
        keystore.setKeyEntry("key-alias", key.getPrivate(), password.toCharArray(), new Certificate[]{cert});

        logger.debug("kafka certificate signed - DN:{}, cert:{}", dn, cert.getType());
    }

    private Properties props;
    private File       keystoreFile;
    private File       truststoreFile;

    /** */
    @Override public String toString() {
        return MessageFormat.format("Pki(host:{0}, appId:{1}, taskId:{2}, token:{3}, dnsName:{4}, password:{5}, keyPair:{6}/{7}, truststore:{8}, keystore:{9} )",
                pkiHost, appId, taskId, token, dnsName, (password != null && !password.isEmpty()) ? "***" : "???", (key == null || key.getPrivate() == null)? "???" : key.getPrivate().getAlgorithm(), (key == null || key.getPublic() == null)? "???" : key.getPublic().getAlgorithm(), (truststore == null)? "???" : truststore.getType(), (keystore == null)? "???" : keystore.getType()
        );
    }

    public Properties getProps()           { return this.props;          }
    public File       getKeystoreFile()    { return this.keystoreFile;   }
    public File       getTruststoreFile()  { return this.truststoreFile; }
    public String     getPassword()        { return this.password;       }
    public AppId      getAppId()           { return this.appId;          }

    /** */
    public void init() { init(false); }
    public void init(Boolean servesSslEndpoint) {
        long now = System.currentTimeMillis();
        try {
            fetchKafkaCert(getDN(), servesSslEndpoint);
            this.props = fetchKafkaProps();
            logger.debug("we got all kafka properties from the PKI: {}", String.join(",", this.props.stringPropertyNames()));

            this.keystoreFile = SslUtils.saveToFile(keystore, password);
            logger.debug("Kafka keystore file created: {}", keystoreFile.toString());

            this.truststoreFile = SslUtils.saveToFile(truststore, password);
            logger.debug("Kafka truststore file created: {}", truststoreFile.toString());
        }
        catch(NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | OperatorCreationException | CertificateException | KeyManagementException | IOException e) {
            throw new IllegalArgumentException("Platform handshake failed", e);
        }

        logger.info("PKI successfully initialized in {} ms", System.currentTimeMillis() - now);
        logger.debug("PKI config - {}", this.toString());

    }
}
