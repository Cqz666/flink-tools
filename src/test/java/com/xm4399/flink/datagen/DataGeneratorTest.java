package com.xm4399.flink.datagen;

import com.xm4399.flink.datagen.pojo.DummyPojo;
import com.xm4399.flink.datagen.pojo.OrderInfo;
import com.xm4399.flink.datagen.pojo.UserInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;


public class DataGeneratorTest {

    private StreamExecutionEnvironment env;
    private DataGenerator dataGenerator;

    @Before
    public void setUp() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        dataGenerator = new DataGenerator<>(env);
    }

    @Test
    public void toDataStream() throws Exception {
        DataGenDescriptor userDescriptor = DataGenDescriptor.forPojo(UserInfo.class)
                .rowsPerSecond(1)
                .numberOfRows(10000)
                .build();
        DataGenDescriptor orderDescriptor = DataGenDescriptor.forPojo(OrderInfo.class)
                .rowsPerSecond(2)
                .numberOfRows(10000)
                .build();

        DataStream<UserInfo> userStream = dataGenerator.toDataStream(userDescriptor);
        DataStream<OrderInfo> orderStream = dataGenerator.toDataStream(orderDescriptor);
        Arrays.stream(dataGenerator.getTableEnv().listTables()).forEach(System.out::println);
        userStream.print();
        orderStream.print();
        env.execute();
    }

    @Test
    public void getTableEnv() {
    }
}