package com.xm4399.flink.constant;

public class Types {

    public static final String STRING = "STRING";

    public static final String INTEGER = "INTEGER";

    public static final String LONG = "LONG";

    public static final String BOOLEAN = "BOOLEAN";

    public static final String BIGDECIMAL = "BIGDECIMAL";

    public static final String FLOAT = "FLOAT";

    public static final String DOUBLE = "DOUBLE";

    public static final String DATE = "DATE";

    public static final String DATETIME = "DATETIME";

    public static final String TIMESTAMP = "TIMESTAMP";


    public static boolean isNumericType(String type){
        return INTEGER.equals(type) ||
                LONG.equals(type) ||
                BIGDECIMAL.equals(type) ||
                FLOAT.equals(type) ||
                DOUBLE.equals(type);
    }

}
