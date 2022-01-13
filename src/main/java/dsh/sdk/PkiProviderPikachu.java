package dsh.sdk;

import dsh.sdk.internal.*;
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
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.Properties;

/**
 * Implements the {@link PkiProvider} that actually uses the platform PKI service to retrieve all required data.
 *
 * Communication with the PKI service will only be established when the required data is not already
 * present in this provider.  After when the data has been fetched once, it is cached locally and the PKI service
 * will not be contacted again.
 */
public class PkiProviderPikachu implements PkiProvider {
    private static final Logger logger = LoggerFactory.getLogger(PkiProviderPikachu.class);

    private final String pkiHost;
    private final String password;
    private final KeyStore keystore;
    private final KeyStore truststore;
    private final Certificate caCert;
    private final KeyPair key;
    private final String token;
    private final AppId appId;
    private final String taskId;
    private final String dnsName;
    private final boolean servesSslEndpoint;

    private PkiProviderPikachu() { throw new AssertionError(); }

    /**
     * Manual initialization of the PkiProvider
     *
     * @param pkiHost
     * @param caCert
     * @param token
     * @param appId
     * @param taskId
     * @throws PkiException
     */
    public PkiProviderPikachu(String pkiHost, Certificate caCert, String token, AppId appId, String taskId) throws PkiException {
        this(pkiHost, caCert, token, appId, taskId, null, false);
    }

    /**
     * Manual initialization of the PkiProvider
     *
     * @param pkiHost
     * @param caCert
     * @param token
     * @param appId
     * @param taskId
     * @param dnsName
     * @param servesSslEndpoint
     * @throws PkiException
     */
    public PkiProviderPikachu(String pkiHost, Certificate caCert, String token, AppId appId, String taskId, String dnsName, boolean servesSslEndpoint) throws PkiException {
        Security.addProvider(new BouncyCastleProvider());

        this.pkiHost = pkiHost;
        this.caCert = caCert;
        this.token = token;
        this.appId = appId;
        this.taskId = taskId;
        this.dnsName = dnsName;
        this.password = SslUtils.generatePassword(32);
        this.servesSslEndpoint = servesSslEndpoint;

        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", BouncyCastleProvider.PROVIDER_NAME);
            keyGen.initialize(2048, new SecureRandom());

            this.key = keyGen.generateKeyPair();
            this.keystore = SslUtils.createKeystore(password, caCert);
            this.truststore = SslUtils.createKeystore(password, caCert);
        }
        catch (CertificateException | IOException | NoSuchProviderException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new PkiException("unable to create PKI handler", e);
        }
    }

    private File keystoreFile;
    private File truststoreFile;
    private Properties props;

    //
    private void handshake() throws PkiException {
        fetchKafkaCert();
        try {
            this.keystoreFile = SslUtils.saveToFile(keystore, password);
            this.truststoreFile = SslUtils.saveToFile(truststore, password);
        }
        catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new PkiException("unable to write KeyStore/TrustStore", e);
        }
    }

    @Override
    public AppId getAppId() { return appId; }

    @Override
    public String getTaskId() { return taskId; }

    @Override
    public String getPassword() { return password; }

    /**
     * Get the keystore (JKS) file.
     * This might trigger comminication with the platform PKI service when
     * the data is not in local cache yet.
     *
     * @return the KeyStore file
     * @throws PkiException when communication with the PkiService fails
     */
    @Override
    public File getKeystoreFile() throws PkiException {
        if(keystoreFile == null) handshake();
        return keystoreFile;
    }

    /**
     * Get the truststore (JKS) file.
     * This might trigger comminication with the platform PKI service when
     * the data is not in local cache yet.
     *
     * @return the TrustStore file
     * @throws PkiException when communication with the PkiService fails
     */
    @Override
    public File getTruststoreFile() throws PkiException {
        if(truststoreFile == null) handshake();
        return truststoreFile;
    }

    /**
     * Get the full set of application properties.
     * This might trigger comminication with the platform PKI service when
     * the data is not in local cache yet.
     *
     * @return all platform properties
     * @throws PkiException when communication with the PkiService fails
     */
    @Override
    public Properties getProperties() throws PkiException {
        if(props == null) {
            if(truststoreFile == null || keystoreFile == null) handshake();
            props = fetchPlatformConfig();
        }
        return props;
    }

    @Override
    public String toString() {
        return String.format( "%s(host: %s)", this.getClass().getName(), pkiHost);
    }

    /** */
    private String getDN() {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) PkiUtils.pkiUrlForPath(pkiHost, PkiUtils.pathForDN(appId.root(), taskId)).openConnection();
            logger.debug("issue http request - {}", conn.getURL().toString());

            if (conn instanceof HttpsURLConnection)
                ((HttpsURLConnection) conn).setSSLSocketFactory(SslUtils.getSocketFactory(truststore, keystore, password));

            return HttpUtils.contentOf(conn);
        }
        catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException | IOException e) {
            throw new PkiException("error while handshaking with platform PKI", e);
        }
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private Properties fetchPlatformConfig() {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) PkiUtils.pkiUrlForPath(pkiHost, PkiUtils.pathForKafkaProps(appId.root(), taskId)).openConnection();
            logger.debug("issue http request - {}", conn.getURL().toString());

            if(conn instanceof HttpsURLConnection) ((HttpsURLConnection)conn).setSSLSocketFactory(SslUtils.getSocketFactory(truststore, keystore, password));
            conn.setRequestProperty("X-Kafka-Config-Token", token);
            return Conversion.convertToProperties(HttpUtils.contentOf(conn));
        }
        catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException | IOException e) {
            throw new PkiException("error while fetching platform config", e);
        }
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private  Certificate sign(PKCS10CertificationRequest csr) throws IOException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, CertificateException {
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
        finally {
            if(conn != null) conn.disconnect();
        }
    }

    /** */
    private void fetchKafkaCert() throws PkiException {
        String dn = getDN();
        String dnsSAN = servesSslEndpoint ? dnsName : null;
        String ipSAN = null;
        if (servesSslEndpoint) {
            Optional<String> ip = getIPAddress();
            if (ip.isPresent()) {
                ipSAN = ip.get();
            }
        }
        try {
            Certificate cert = sign(SslUtils.buildCsr(dn, key, dnsSAN, ipSAN));
            keystore.setCertificateEntry("server", cert);
            keystore.setKeyEntry("key-alias", key.getPrivate(), password.toCharArray(), new Certificate[]{cert});

            logger.debug("kafka certificate signed - DN:{}, cert:{}", dn, cert.getType());
        }
        catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyManagementException | CertificateException | KeyStoreException | OperatorCreationException | IOException e) {
            throw new PkiException("error while acquiring Kafka access", e);
        }
    }

    /**
     * Find our IP address.
     *
     * We set up a connection to Pikachu, and then query the connection's local address.
     * That ensures we get the IP address that matches what pikachu sees when we interact with it,
     * because that it the only IP address it will allow as a SAN in the CSR.
     */
    private Optional<String> getIPAddress() {
        try {
            final URL url = new URL(pkiHost);
            try (final Socket socket = new Socket(url.getHost(), url.getPort())) {
                final String ip = socket.getLocalAddress().getHostAddress();
                return Optional.of(ip);
            }
        } catch (Exception e) {
            logger.warn("could not detect IP address: {}", e);
            return Optional.empty();
        }
    }
}
