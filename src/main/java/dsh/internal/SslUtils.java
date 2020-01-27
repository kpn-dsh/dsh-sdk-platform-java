package dsh.internal;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

/**
 *
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
     *
     * @param pem
     * @return
     * @throws IOException
     */
    public static String stringFromPEM(Object pem) throws IOException {
        StringWriter stringWriter = new StringWriter();
        try(JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) { pemWriter.writeObject(pem); }

        String pemAsString = stringWriter.toString();

        logger.debug("PEM file decoded as string: {} characters", pemAsString.length());
        return pemAsString;
    }

    /**
     *
     * @param pem
     * @return
     * @throws CertificateException
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

            byte[] buf = DatatypeConverter.parseBase64Binary(tokens[0]);

            logger.debug("raw content extracted from pem-length:{}, content-size:{}", pem.length(), buf.length);
            return buf;
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid PEM file content", e);
        }
    }

    /**
     *
     * @param truststore
     * @param keystore
     * @param password
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     * @throws UnrecoverableKeyException
     * @throws KeyManagementException
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
     *
     * @param password
     * @param ca
     * @return
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws IOException
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
     * @param dn
     * @param key
     * @param dns
     * @return
     * @throws OperatorCreationException
     * @throws IOException
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
     *
      * @param store
     * @param password
     * @return
     * @throws IOException
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     */
    public static File saveToFile(KeyStore store, String password) throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        File file = File.createTempFile("__secret_store__", "." + store.getType().toLowerCase());
        try(FileOutputStream fos = new FileOutputStream(file)) { store.store(fos, password.toCharArray()); }

        logger.debug("keystore saved to file: {}", file.toString());
        return file;
    }
}
