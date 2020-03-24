package dsh.sdk.internal;

import dsh.messages.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AppId -- represents all information to identify an application
 */
public class AppId {
    private static final Logger logger = LoggerFactory.getLogger(AppId.class);

    private static Pattern appIdSplitter = Pattern.compile("^[/]{0,1}([^/]+(?:/[^/]+)*)/([^/]+)$");
    private static final int APPID_GROUPS_IDX = 1;
    private static final int APPID_NAME_IDX = 2;

    private static boolean isValidAppId(String appId) {
        return Optional.ofNullable(appId)
                .map(appIdSplitter::matcher)
                .filter(Matcher::find)
                .filter(m -> !m.group(APPID_GROUPS_IDX).isEmpty())
                .filter(m -> !m.group(APPID_NAME_IDX).isEmpty())
                .isPresent();
    }

    private static String[] groupsFromAppId (String appId) {
        return Optional.of(appIdSplitter.matcher(appId))
                .filter(Matcher::find)
                .map(m -> m.group(APPID_GROUPS_IDX))
                .map(s -> s.split("/"))
                .get();
    }

    private static String nameFromAppId(String appId) {
        return Optional.of(appIdSplitter.matcher(appId))
                .filter(Matcher::find)
                .map(m -> m.group(APPID_NAME_IDX))
                .get();
    }

    private AppId() { throw new AssertionError(); }
    private AppId(String appId) {
        if(! isValidAppId(appId)) throw new IllegalArgumentException();
        this.appId = appId;
    }

    private final String appId;

    /**
     * The application group-chain
     *
     * An application is deployed within a group (or subgroup)
     * with the first node being the tenant identifier.
     *
     * @return ordered set of the application groups
     */
    public Set<String> groups() { return new HashSet<>(Arrays.asList(groupsFromAppId(appId))); }

    /**
     * The root-node of the group-chain the application resides in.
     * (is equal to the tenant network)
     *
     * @return root node of the group-chain (= tenant identifier)
     */
    public String root() { return groupsFromAppId(appId)[0]; }

    /**
     * The application name as it is known on the platform.
     *
     * @return Application name
     */
    public String name() { return nameFromAppId(appId); }

    /**
     * The {@link dsh.messages.Envelope.Identity} to use when publishing data on the platform.
     *
     * @return KeyEnvelope identity (data owner)
     */
    public Envelope.Identity identity() {
        return Envelope.Identity.newBuilder().setTenant(root()).setApplication(name()).build();
    }

    @Override
    public String toString() {
        return "appId: " + appId;
    }

    /**
     * Parses the provided string into an AppId.
     *
     * The String needs to comply to the following structure:
     *
     * {@code /my-tenant-id/sub1/sub2/.../my-app}
     * {@code /my-tenant-id/my-app}
     * {@code my-tenant-id/sub1/sub2/.../my-app}
     * {@code my-tenant-id/my-app}
     *
     * @param appId textual representation of the application identifier
     * @return AppId
     * @exception IllegalArgumentException when not a valid application id
     */
    public static AppId from(String appId) {
        return new AppId(appId);
    }

    /**
     * Create an AppId from the given name and group-chain.
     *
     * @param appName  name for the application
     * @param groups   application group-chain
     * @return AppId
     * @exception IllegalArgumentException when the AppId could not be generated
     */
    public static AppId from(String appName, String... groups) {
        return new AppId(
                String.join("/", groups) + "/" + appName
        );
    }
}
