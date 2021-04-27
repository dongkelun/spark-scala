package com.dkl.learning.reflect.main;

/**
 * Created by dongkelun on 2021/4/25 15:17
 */
public class Student {

    public static void main(String[] args) {
        System.out.println("main方法执行了。。。");
        for(String arg:args){
            System.out.println(arg);
        }
    }
}
