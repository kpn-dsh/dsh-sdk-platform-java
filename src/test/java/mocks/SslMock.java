package mocks;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.Certificate;
import java.util.Date;

public class SslMock {
    private static final String KeyStoreType = "JKS";
    private static final String Provider = "BC";
    private static final String Algorithm = "RSA";
    private static final String SignatureAlgorithm = "SHA256WithRSAEncryption";

    public static void init() {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static X500Name DN(String tenant) {
        X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
        x500NameBuilder.addRDN(BCStyle.CN, "container." + tenant);
        x500NameBuilder.addRDN(BCStyle.OU, tenant);
        x500NameBuilder.addRDN(BCStyle.O, "dsh");
        return x500NameBuilder.build();
    }

    public static Certificate createCertificate(String alias) {
        try {
            KeyPairGenerator kpGen = KeyPairGenerator.getInstance(Algorithm, Provider);
            kpGen.initialize(1024, new SecureRandom());
            KeyPair pair = kpGen.generateKeyPair();
            X500Name issuer = DN(alias);

            JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuer, BigInteger.valueOf(System.currentTimeMillis()), new Date(System.currentTimeMillis() - 86400000L), new Date(System.currentTimeMillis() + 1827387392L), issuer, pair.getPublic());
            PrivateKey signingPrivateKey = pair.getPrivate();
            ContentSigner sigGen = new JcaContentSignerBuilder(SignatureAlgorithm).setProvider(Provider).build(signingPrivateKey);
            Certificate x509Cert = new JcaX509CertificateConverter().setProvider(Provider).getCertificate(builder.build(sigGen));

            return x509Cert;
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
