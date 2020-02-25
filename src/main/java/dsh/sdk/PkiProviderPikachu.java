package dsh.sdk;

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
import java.util.Properties;

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

    @Override
    public File getKeystoreFile() throws PkiException {
        if(keystoreFile == null) handshake();
        return keystoreFile;
    }

    @Override
    public File getTruststoreFile() throws PkiException {
        if(truststoreFile == null) handshake();
        return truststoreFile;
    }

    @Override
    public Properties getProperties() throws PkiException {
        if(props == null) props = fetchPlatformConfig();
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
        try {
            Certificate cert = sign(SslUtils.buildCsr(dn, key, servesSslEndpoint? dnsName : null));
            keystore.setCertificateEntry("server", cert);
            keystore.setKeyEntry("key-alias", key.getPrivate(), password.toCharArray(), new Certificate[]{cert});

            logger.debug("kafka certificate signed - DN:{}, cert:{}", dn, cert.getType());
        }
        catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyManagementException | CertificateException | KeyStoreException | OperatorCreationException | IOException e) {
            throw new PkiException("error while acquiring Kafka access", e);
        }
    }
}
