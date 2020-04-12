package com.uestc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class JavaHelloWorld {
    public static void main(String[] args){
        String inputPath = "file:///C:/Users/admin/Desktop/in/inputData.txt";
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("persist");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //必须在创建rdd后连续调用cache，或者persist
        //如果另起一行是没有效果的
        //JavaRDD<String> lines = sc.textFile(inputPath).cache();
        JavaRDD<String> lines = sc.textFile(inputPath).persist(StorageLevel.MEMORY_AND_DISK());
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        long endTime = System.currentTimeMillis();
        System.out.println("count1 cost time:" + (endTime - beginTime));
        beginTime = System.currentTimeMillis();
        count = lines.count();
        endTime = System.currentTimeMillis();
        System.out.println("count2 cost time:" + (endTime - beginTime));

        sc.close();
    }
}
