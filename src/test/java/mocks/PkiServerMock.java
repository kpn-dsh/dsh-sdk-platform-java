package mocks;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.security.cert.Certificate;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;

public class PkiServerMock {

    public PkiServerMock(int port, Certificate ca, String kafkaProps) {
        try {
            this.ca = ca;
            this.kafkaProps = kafkaProps;
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.setExecutor(Executors.newSingleThreadExecutor());
            this.server.createContext("/dn", this::handleDnRequest);
            this.server.createContext("/kafka/config", this::handleKafkaRequest);
            this.server.createContext("/sign", this::handleSignRequest);
        }
        catch (IOException e) {
            fail();
        }
    }

    public void start() { server.start(); }
    public void stop() { server.stop(5); }

    public void setTenant(String tenant) { this.tenant = tenant; }

    private HttpServer server;
    private String tenant;
    private Certificate ca;
    private String kafkaProps;

    public String connectionString() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    /** */
    private void handleDnRequest(HttpExchange http) throws IOException {
        http.sendResponseHeaders(200, SslMock.DN(this.tenant).toString().getBytes(StandardCharsets.UTF_8).length);
        try(OutputStream os = http.getResponseBody()) { os.write(SslMock.DN(this.tenant).toString().getBytes(StandardCharsets.UTF_8)); }
        http.close();
    }

    /** */
    private void handleKafkaRequest(HttpExchange http) throws IOException {
        try {


            http.sendResponseHeaders(200, kafkaProps.getBytes(StandardCharsets.UTF_8).length);
            try (OutputStream os = http.getResponseBody()) {
                os.write(kafkaProps.getBytes(StandardCharsets.UTF_8));
            }
        }
        finally { http.close(); }
    }

    /** */
    private void handleSignRequest(HttpExchange http) throws IOException {

        try {
            PKCS10CertificationRequest csr = convertPemToPKCS10CertificationRequest(http.getRequestBody());

            //check RDN and respond with 403 if not matching tenant-id.
            if(! csr.getSubject().equals(SslMock.DN(this.tenant))) {
                http.sendResponseHeaders(403, -1);
                return;
            }

            //FIXME: we just return the provided certificate as PEM file
            StringWriter writer = new StringWriter();
            JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
            pemWriter.writeObject(ca);
            pemWriter.close();

            http.sendResponseHeaders(200, writer.toString().getBytes(StandardCharsets.UTF_8).length);
            try (OutputStream os = http.getResponseBody()) {
                http.getResponseBody().write(writer.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        catch (Exception e) { e.printStackTrace(); }
        finally { http.close(); }
    }

    /** */
    private PKCS10CertificationRequest convertPemToPKCS10CertificationRequest(InputStream pem) throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        PKCS10CertificationRequest csr = null;

        Reader pemReader = new BufferedReader(new InputStreamReader(pem));
        try(PEMParser pemParser = new PEMParser(pemReader)) {
            Object parsedObj = pemParser.readObject();
            if (parsedObj instanceof PKCS10CertificationRequest) {
                csr = (PKCS10CertificationRequest) parsedObj;
            }
        }

        return csr;
    }
}
