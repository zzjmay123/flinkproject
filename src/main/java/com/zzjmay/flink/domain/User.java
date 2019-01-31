package com.zzjmay.flink.domain;

import java.io.Serializable;

/**
 * Created by zzjmay on 2019/1/31.
 */
public class User implements Serializable {

    private Long userId;

    private String name;

    private String project;

    private int score;

    public User() {
    }

    public User(Long userId, String name, String project, int score) {
        this.userId = userId;
        this.name = name;
        this.project = project;
        this.score = score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }
}
