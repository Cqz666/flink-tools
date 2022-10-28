package com.cqz.flink.datagen.pojo;

import com.cqz.flink.datagen.DataGenOption;
import lombok.Data;

@Data
public class UserInfo {

    @DataGenOption(kind = "sequence", start=1,end=100000)
    private Long id;

    @DataGenOption(length = 16)
    private String name;

    @DataGenOption(min=1,max=100)
    private Integer age;

    @DataGenOption(min=0,max=1)
    private Integer sex;

    private Boolean isBlack;

}
