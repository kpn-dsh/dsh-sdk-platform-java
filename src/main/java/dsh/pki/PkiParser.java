package dsh.pki;

import dsh.internal.AppId;

/**
 *
 */
public class PkiParser {
    /** */
    private static Pki validate(Pki pki) {
        //TODO: checks ...
        return pki;
    }

    private final Pki pki;
    private PkiParser(Pki pki) { this.pki = pki; }

    /**
     *
     * @param pki
     * @return
     */
    public static PkiParser parse(Pki pki) { return new PkiParser(validate(pki)); }

    /**
     *
     * @return
     */
    public AppId application() { return pki.getAppId(); }
}
