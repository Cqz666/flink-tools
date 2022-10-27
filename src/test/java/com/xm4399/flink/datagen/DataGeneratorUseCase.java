package com.xm4399.flink.datagen;

import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorUseCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGenerator dataGenerator = new DataGenerator<>(env);
        DataGenDescriptor descriptor = DataGenDescriptor.forPojo(UserInfo.class)
                .rowsPerSecond(10)
                .numberOfRows(10000)
                .build();
        DataStream<UserInfo> userStream = dataGenerator.toDataStream(descriptor);
        userStream.print();
        env.execute();
    }

    @Data
    public static class UserInfo {
        @DataGenOption(kind = "sequence", start=1,end=10000)
        private Long id;

        @DataGenOption(length = 16)
        private String name;

        @DataGenOption(min=1,max=100)
        private Integer age;

        @DataGenOption(min=0,max=1)
        private Integer sex;

        private Boolean isBlack;
    }

}

