package com.dkl.blog.spark.df;

import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag$;

/**
 * Java版建立空的DataFrame
 */
public class EmptyDataFrame_Java {
    public static void main(String[] args) {
        System.out.println("test java");
        SparkSession spark = SparkSession.builder().appName("EmptyDataFrame_Java").master("local").getOrCreate();
        String[] colNames = new String[]{"id", "name", "age", "birth"};


        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.StringType, false, Metadata.empty()),
                new StructField("birth", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> emptyDf = spark.createDataFrame(spark.sparkContext().emptyRDD(ClassTag$.MODULE$.apply(Row.class)), schema);
        emptyDf.show();
        //以下三种写法等价，参考：https://blog.csdn.net/hhtop112408/article/details/78338716
        ClassManifestFactory.classType(Row.class);
        ClassTag$.MODULE$.apply(Row.class);
        JavaSparkContext$.MODULE$.fakeClassTag();
        spark.emptyDataFrame().show();
        spark.stop();

    }

}

