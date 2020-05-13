package dsh.sdk.internal;

import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * General purpose SSL helper functions
 */
public class SslUtils {
    private static final Logger logger = LoggerFactory.getLogger(SslUtils.class);
    private static final String passwordCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /** generates a random password of 'length' characters */
    public static String generatePassword(Integer length) {
        if (length <= 0) {
            throw new IllegalArgumentException("String length must be a positive integer");
        }

        Random random = new SecureRandom();
        return Arrays.stream(new Character[length])
                .map(c -> passwordCharacters.charAt(random.nextInt(passwordCharacters.length())))
                .map(String::valueOf)
                .collect(Collectors.joining());
    }

    /**
     * Tries to parse the provided Object as a PEM object.
     * OpenSSL PEM objects based on JCA/JCE classes
     *
     * @param pem   the object representing the PEM object.
     * @return      String representation of the PEM object
     * @throws IOException when the object could not be parsed to a String
     */
    public static String stringFromPEM(Object pem) throws IOException {
        StringWriter stringWriter = new StringWriter();
        try(JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) { pemWriter.writeObject(pem); }

        String pemAsString = stringWriter.toString();

        logger.debug("PEM file decoded as string: {} characters", pemAsString.length());
        return pemAsString;
    }

    /**
     * Generates a certificate object and initializes it with the data read from the input string.
     *
     * @param pem String representation of the certificate to create
     * @return X509 Certificate
     * @throws CertificateException when the provided string does not represent a valid certificate
     */
    public static Certificate certificateFromPEM(String pem) throws CertificateException {
        return CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(rawContentFromPEM(pem)));
    }

    /** */
    private static byte[] rawContentFromPEM(String pem) {
        final String PEM_START = "-----BEGIN CERTIFICATE-----";
        final String PEM_END   = "-----END CERTIFICATE-----";

        try {
            String[] tokens = pem.split(PEM_START);
            tokens = tokens[1].split(PEM_END);

            byte[] buf = Base64.decode(tokens[0]);

            logger.debug("raw content extracted from pem-length:{}, content-size:{}", pem.length(), buf.length);
            return buf;
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid PEM file content", e);
        }
    }

    /**
     * Creates a socket factory used to setup TLS connections from the provided Key- and Truststores.
     *
     * @param truststore truststore to use for the SSL socket factory
     * @param keystore   keystore to use for the SSL socket factory
     * @param password   password to access the key- and truststore.
     * @return SSL socketfactory to create SSL sockets with
     * @throws NoSuchAlgorithmException when the X509 cryptographic algorithm is not available
     * @throws KeyStoreException when an error occurred accessing the KeyStore
     * @throws UnrecoverableKeyException when a key in the keystore could not be recovered
     * @throws KeyManagementException This is the general key management exception for all operations dealing with key management
     *                                to indicate that the created SSLContext could not be correctly initialized.
     */
    public static SSLSocketFactory getSocketFactory(KeyStore truststore, KeyStore keystore, String password) throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException {
        SSLContext context = SSLContext.getInstance("TLS");
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(truststore);
        TrustManager[] tm = tmf.getTrustManagers();

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keystore, password.toCharArray());

        KeyManager[] km = kmf.getKeyManagers();
        context.init(km, tm, null);

        logger.debug("ssl socket factory created: {}", context.getProtocol());
        return context.getSocketFactory();
    }

    /**
     * Create a new KeyStore and initialize it with a root CA
     *
     * @param password the password to access the keystore with
     * @param ca root CA that needs to be added to the keystore
     * @return the initialized KeyStore
     * @throws NoSuchAlgorithmException when the X509 cryptographic algorithm is not available
     * @throws KeyStoreException when an error occurred accessing the KeyStore
     * @throws CertificateException when one of a variety of certificate problems was encountered
     * @throws IOException general purpose exception indicating something went wrong accessing the KeyStore
     */
    public static KeyStore createKeystore(String password, Certificate ca) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore store = KeyStore.getInstance("JKS");
        store.load(null, password.toCharArray());
        store.setCertificateEntry("ca", ca);

        logger.debug("keystore created: {}", store.getType());
        return store;
    }

    /**
     *
     * @param dn X500 DNname
     * @param key  key pair to sign with
     * @param dns DNS name (optional)
     * @return Certificate signing request
     * @throws OperatorCreationException when the certificate signer could not be created
     * @throws IOException when ASN.1 encoding failed
     */
    public static PKCS10CertificationRequest buildCsr(String dn, KeyPair key, String dns) throws OperatorCreationException, IOException {
        JcaPKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(new X500Name(dn), key.getPublic());
        if(dns != null && !dns.isEmpty()) {
            GeneralNames subjectAltNames = new GeneralNames(new GeneralName(GeneralName.dNSName, dns));
            ExtensionsGenerator extGen = new ExtensionsGenerator();
            extGen.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
            builder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, extGen.generate());
        }

        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSAEncryption")
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(key.getPrivate());

        PKCS10CertificationRequest csr = builder.build(signer);

        logger.debug("CSR built: {}", csr.getSubject().toString());
        return csr;
    }

    /**
     * Saves the Keystore to a local temp file.
     * The file is automatically created with following name: <i>__secret_store__[UUID].jks</i>
     *
     * @param store the Keystore to save
     * @param password the password needed to access the Keystore
     * @return File referencing the newly created temp file containing the KeyStore
     * @throws IOException when reading the keystore or writing the file somehow fails
     * @throws NoSuchAlgorithmException when the X509 cryptographic algorithm is not available
     * @throws KeyStoreException when an error occurred accessing the KeyStore
     * @throws CertificateException when one of a variety of certificate problems was encountered
     */
    public static File saveToFile(KeyStore store, String password) throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        File file = File.createTempFile("__secret_store__", "." + store.getType().toLowerCase());
        try(FileOutputStream fos = new FileOutputStream(file)) { store.store(fos, password.toCharArray()); }

        logger.debug("keystore saved to file: {}", file.toString());
        return file;
    }
}
