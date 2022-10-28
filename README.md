## flink-tools 工具包
### 1.  DataGenerator (DataStream模拟数据生成器）
* 通过声明式的API结合POJO自定义注解，快速生成DataStream模拟数据
* 支持datagen连接器所有option

## Getting started
### 1. Building
```shell script
mvn clean install
```
### 2. import
```xml
<dependency>
    <groupId>com.cqz</groupId>
    <artifactId>flink-tools</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Use case
### 1. DataGenerator
```java
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
```