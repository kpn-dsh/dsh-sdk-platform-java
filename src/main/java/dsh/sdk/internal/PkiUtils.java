package dsh.sdk.internal;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * DSH specific helper functions for accessing the platforms PKI service
 */
public class PkiUtils {
    /**
     * Create the URL required for signing the certificate signing request.
     *
     * @param group   application group
     * @param taskId  the applications task-id as it is known on the platform.
     * @return        URL path to send the CSR to.
     */
    public static String pathForSigning(String group, String taskId) {
        return "/sign/" + group + "/" + taskId;
    }

    /**
     * The path on the platforms PKI service used to validate this application instance (task)
     *
     * @param group   application group
     * @param taskId  the applications task-id as it is known on the platform.
     * @return        String representing the path to access the PKI service with
     */
    public static String pathForDN(String group, String taskId) {
        return "/dn/" + group + "/" + taskId;
    }

    /**
     * Create a URL from the given path for the provided PKI service URI.
     *
     * @param pkiHost   the URL of the PKI service of the platform
     * @param path      the service path you want to access on the PKI service
     * @return          full URL of the platforms PKI service for the given path
     * @exception       IllegalArgumentException when no valid URL could be formed.
     */
    public static URL pkiUrlForPath(String pkiHost, String path) {
        try {
            if(pkiHost.startsWith("http://") || pkiHost.startsWith("https://")) { return new URL(pkiHost + path); }
            return new URL("https://" + pkiHost + path);
        }
        catch(MalformedURLException e) {
            throw new IllegalArgumentException("Invalid PKI URL", e);
        }
    }

    /**
     * The path on the plafroms PKI service used to fetch Kafka properties for the application
     *
     * @param group   application group
     * @param taskId  the applications task-id as it is known on the platform.
     * @return        String representing the path to access the PKI service with
     */
    public static String pathForKafkaProps(String group, String taskId) {
        return "/kafka/config/" + group + "/" + taskId + "?format=java";
    }
}
