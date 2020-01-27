package dsh.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Conversion {

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