package dsh.internal;

import dsh.messages.common.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class AppId {
    private static final Logger logger = LoggerFactory.getLogger(AppId.class);

    private static Pattern appIdSplitter = Pattern.compile("^/([^/]+(?:/[^/]+)*)/([^/]+)$");
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
    private AppId(String appId) { this.appId = appId; }

    private final String appId;

    /**
     *
     * @return
     */
    public Set<String> groups() { return new HashSet<>(Arrays.asList(groupsFromAppId(appId))); }

    /**
     *
     * @return
     */
    public String root() { return groupsFromAppId(appId)[0]; }

    /**
     *
     * @return
     */
    public String name() { return nameFromAppId(appId); }

    /**
     *
     * @return
     */
    public Envelope.Identity identity() {
        return Envelope.Identity.newBuilder().setTenant(root()).setPublisher(name()).build();
    }

    @Override
    public String toString() {
        return "appId: " + appId;
    }

    /**
     *
     * @param appId
     * @return
     */
    public static AppId from(String appId) {
        if(isValidAppId(appId)) return new AppId(appId);
        else throw new IllegalArgumentException("invalid application-id");
    }

}
