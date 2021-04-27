package com.dkl.learning.reflect.field;

/**
 * Created by dongkelun on 2021/4/25 11:19
 */
public class Student {
    public Student(){

    }
    //**********字段*************//
    public String name;
    protected int age;
    char sex;
    private String phoneNum;

    @Override
    public String toString() {
        return "Student [name=" + name + ", age=" + age + ", sex=" + sex
                + ", phoneNum=" + phoneNum + "]";
    }
}
