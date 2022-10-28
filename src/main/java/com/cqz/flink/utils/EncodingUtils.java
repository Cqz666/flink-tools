package com.cqz.flink.utils;

import org.apache.flink.configuration.GlobalConfiguration;

import static org.apache.flink.configuration.GlobalConfiguration.HIDDEN_CONTENT;

public class EncodingUtils {

    private EncodingUtils() {
        // do not instantiate
    }

    public static String escapeSingleQuotes(String s) {
        return s.replace("'", "''");
    }

    public static String stringifyOption(String key, String value) {
        if (GlobalConfiguration.isSensitive(key)) {
            value = HIDDEN_CONTENT;
        }
        return String.format(
                "'%s'='%s'",
                EncodingUtils.escapeSingleQuotes(key),
                EncodingUtils.escapeSingleQuotes(value));
    }

}
