package dsh.sdk.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * General purpose conversion functions
 */
public class Conversion {

    /**
     * Convert a character stream to Java Properties
     *
     * @param s String representation of Properties
     * @return Properties
     * @exception IllegalArgumentException when the provided String can not be parsed as a list of Java Properties
     * @see Properties
     */
    public static Properties convertToProperties(String s) {
        try (InputStream propStream = new ByteArrayInputStream(s.getBytes())) {
            Properties props = new Properties();
            props.load(propStream);
            return props;
        } catch (IOException e) {
            throw new IllegalArgumentException("can not convert string to properties", e);
        }
    }
}