package com.xm4399.flink.datagen;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DataGenOption {

    String kind() default "random";

    int length() default 100;

    long min() default Long.MIN_VALUE;

    long max() default Long.MAX_VALUE;

    long start() default 0;

    long end() default Long.MAX_VALUE;

}
