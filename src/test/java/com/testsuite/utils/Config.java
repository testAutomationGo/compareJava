package com.testsuite.utils;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties p = new Properties();
    static {
        try (InputStream is = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (is != null) p.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static String get(String key) {
        String v = System.getProperty(key);
        if (v != null && !v.isBlank()) return v;
        return p.getProperty(key);
    }
}



