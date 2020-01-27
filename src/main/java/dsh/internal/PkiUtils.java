package dsh.internal;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 */
public class PkiUtils {
    /**
     *
     * @param group
     * @param taskId
     * @return
     */
    public static String pathForSigning(String group, String taskId) {
        return "/sign/" + group + "/" + taskId;
    }

    /**
     *
     *
     * @param group
     * @param taskId
     * @return
     */
    public static String pathForDN(String group, String taskId) {
        return "/dn/" + group + "/" + taskId;
    }

    /**
     *
     * @param pkiHost
     * @param path
     * @return
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
     *
     * @param group
     * @param taskId
     * @return
     */
    public static String pathForKafkaProps(String group, String taskId) {
        return "/kafka/config/" + group + "/" + taskId + "?format=java";
    }
}
