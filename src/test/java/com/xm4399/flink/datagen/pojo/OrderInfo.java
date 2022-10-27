package com.xm4399.flink.datagen.pojo;

import com.xm4399.flink.datagen.DataGenOption;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class OrderInfo {

    @DataGenOption(min=1,max=100000)
    private Integer id;

    @DataGenOption(length = 16)
    private String name;

    @DataGenOption(min=1,max=100000)
    private Long userId;

    @DataGenOption(min=1,max=100000)
    private Double totalAmount;

    private Timestamp createTime;

}
