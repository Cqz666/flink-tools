package com.xm4399.flink.datagen;

import org.apache.flink.util.Preconditions;

public class DataGenDescriptor {
    private final Class<?> pojoClass;
    private final Long rowsPerSecond;
    private final Long numberOfRows;

    private DataGenDescriptor(Class<?> pojoClass, Long rowsPerSecond, Long numberOfRows) {
        this.pojoClass = pojoClass;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
    }

    public static Builder forPojo(Class<?> pojoClass){
        Preconditions.checkNotNull(pojoClass, "Data Generator require a pojo class.");
        final Builder builder = new Builder();
        builder.pojoClass(pojoClass);
        return builder;
    }

    public Class<?> getPojoClass(){
        return pojoClass;
    }

    public Long getRowsPerSecond() {
        return rowsPerSecond;
    }

    public Long getNumberOfRows() {
        return numberOfRows;
    }

    public static class Builder{
        private Class<?> pojoClass;
        private Long rowsPerSecond;
        private Long numberOfRows;

        private Builder(){
        }

        public Builder pojoClass(Class<?> pojoClass){
            this.pojoClass = pojoClass;
            return this;
        }

        public Builder rowsPerSecond(long rowsPerSecond){
            this.rowsPerSecond = rowsPerSecond;
            return this;
        }

        public Builder numberOfRows(long numberOfRows){
            this.numberOfRows = numberOfRows;
            return this;
        }

        public DataGenDescriptor build(){
            return new DataGenDescriptor(pojoClass, rowsPerSecond, numberOfRows);
        }

    }
}
