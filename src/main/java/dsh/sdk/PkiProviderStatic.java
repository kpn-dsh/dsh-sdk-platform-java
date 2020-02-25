package dsh.sdk;

import dsh.internal.AppId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PkiProviderStatic implements PkiProvider {
    private static final Logger logger = LoggerFactory.getLogger(PkiProviderStatic.class);

    private PkiProviderStatic() { throw new AssertionError(); }
    private final Properties props;
    private static final Pattern AppTaskRegex = Pattern.compile("^([^.]+)\\.([^_]+)_.*");

    //
    private static String idFromProps(int group, Properties props) {
        String x = Arrays.stream(props.getProperty("consumerGroups.private").split(","))
                .findFirst()
                .orElseThrow(NoSuchElementException::new);

        Matcher matcher = AppTaskRegex.matcher(x);
        if(matcher.find()) {
            return matcher.group(group);
        }
        else throw new NoSuchElementException();
    }

    private static String taskIdFromProps(Properties props)   { return idFromProps(2, props); }
    private static String appIdFromProps(Properties props)    { return idFromProps(1, props).replaceAll("_", "/"); }
    private static String passwordFromProps(Properties props) { return Optional.ofNullable(props.getProperty("ssl.key.password")).orElseThrow(NoSuchElementException::new); }
    private static String keystoreFromProps(Properties props) { return Optional.ofNullable(props.getProperty("ssl.keystore.location")).orElseThrow(NoSuchElementException::new); }
    private static String truststoreFromProps(Properties props) { return Optional.ofNullable(props.getProperty("ssl.truststore.location")).orElseThrow(NoSuchElementException::new); }

    /**
     *
     * @param allProps
     */
    public PkiProviderStatic(Properties allProps) {
        this.props = allProps;
    }

    @Override
    public AppId getAppId() {
        return AppId.from(appIdFromProps(props));
    }

    @Override
    public String getTaskId() {
        return taskIdFromProps(props);
    }

    @Override
    public String getPassword() {
        return passwordFromProps(props);
    }

    @Override
    public File getKeystoreFile() throws PkiException {
        return new File(keystoreFromProps(props));
    }

    @Override
    public File getTruststoreFile() throws PkiException {
        return new File(truststoreFromProps(props));
    }

    @Override
    public Properties getProperties() throws PkiException {
        Properties p = new Properties();
        p.putAll(props);
        return p;
    }

    @Override
    public String toString() {
        return String.format( "%s(props:%d)", this.getClass().getName(), props.size());
    }
}
