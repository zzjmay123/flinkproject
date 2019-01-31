package com.zzjmay.flink.domain;

import java.io.Serializable;

/**
 * 结果表
 * Created by zzjmay on 2019/1/31.
 */
public class UserResult implements Serializable {



    private String name;

    private int sumNum;

    public UserResult() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSumNum() {
        return sumNum;
    }

    public void setSumNum(int sumNum) {
        this.sumNum = sumNum;
    }
}
