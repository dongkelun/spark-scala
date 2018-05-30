package com.hs.xlzf.task.credit

import com.hs.xlzf.Utils.SparkUtil
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.types.StringType
import com.hs.xlzf.Utils.WOE
import org.apache.spark.ml.param.ParamPair

object DK_A{
    def main(args: Array[String]): Unit = {
//      val sc = SparkUtil.init_sc("DK_A")
        val spark = SparkUtil.init_spark("DK_A")  //spark是SparkSession对象
//        val df = spark.read.parquet("hdfs://10.180.29.180:9000/user/root/spark/spark-warehouse/dk_order_result_dkl").cache
        val df = spark.read.parquet("dk_order_result_dkl").cache

        import org.apache.spark.sql.functions._
        def f(i: Int) = {
            if (i > 0) {
                1
            } else {
                0
            }
        }

        val udf1 = udf(f _)
        val df1 = df.
            withColumn("num1_p", df("NUM1") / df("TOTAL_NUM")). //p:代扣1次成功占比
            withColumn("num2_p", df("NUM2") / df("TOTAL_NUM")). //p:代扣2次成功占比
            withColumn("num3_p", df("NUM3") / df("TOTAL_NUM")). //p:代扣3次成功占比
            withColumn("num4_p", df("NUM4") / df("TOTAL_NUM")). //p:代扣>3次成功占比
            withColumn("num_fail_p", df("NUM_FAIL") / df("TOTAL_NUM")). //p:代扣不成功占比
            withColumn("y", udf1(df("NUM4"))). //y:是否违约(存在化代扣次数大于3的记录),1是，0否
            withColumn("p", df("NUM4") / df("TOTAL_NUM")) //p:违约率
        val T = df1.count()
        val B = df1.agg(sum("y")).first.getLong(0)

        val woe1  = new WOE()
        woe1.setInputCols(Array("g1","g2","g3","g4"));
        woe1.setOutputCols(Array("woe11","woe12","woe13","woe14"));
        woe1.setLabelCol("y")

        import org.apache.spark.ml.feature.QuantileDiscretizer
        import org.apache.spark.ml.feature.Bucketizer

        //NUM1的WoE值woe1
        val qd1 = new QuantileDiscretizer().setInputCol("NUM1").setOutputCol("g1").
        setNumBuckets(5).fit(df1)
        val df2 = qd1.transform(df1)
        val df3 = woe(df2, "y", "g1", "woe1")

        //num1_p的WoE值woe2
        val qd2 = new Bucketizer().setInputCol("num1_p").setOutputCol("g2").
        setSplits(Array(0.0, 0.8, 0.9, 0.95,  0.98, 0.99, 1.0))
        val df4 = qd2.transform(df3)
        //num1_p的WoE值woe2
        val df5 = woe(df4, "y", "g2", "woe2")
        //TOTAL_NUM的WoE值woe3
        val qd3 = new QuantileDiscretizer().setInputCol("TOTAL_NUM").setOutputCol("g3").
        setNumBuckets(5).fit(df5)
        val df6 = qd3.transform(df5)
        val df7 = woe(df6, "y", "g3", "woe3")
        //AVG_AMT的WoE值woe4
        val qd4 = new QuantileDiscretizer().setInputCol("AVG_AMT").setOutputCol("g4").
        setNumBuckets(5).fit(df7)
        val df8 = qd4.transform(df7)
        val df9 = woe(df8, "y", "g4", "woe4")

//        val ds = spark.read.parquet("spark/spark-warehouse/dk_woe").cache
        val ds = df9
        val y_1 = ds.where("y=1")
        val y_0 = ds.where("y=0")

        val y_0_1 = y_0.sample(false, 0.13)  //解决类别数据平衡性问题，对没有违约样本进行随机抽样

        val y_all = y_0_1.union(y_1)

        val dfWoe = woe1.fit(ds).transform(ds)
        dfWoe.show()
        val Array(train, test) = y_all.randomSplit(Array(0.8, 0.2))
 return
        val fcol = Array("woe3", "woe2", "woe4")  //使用总通行次数，1次代扣成功率，平均代扣金额 三个维度的WoE值作为特征向量

        import org.apache.spark.ml.feature.VectorAssembler
        val va = new VectorAssembler().setOutputCol("features").setInputCols(fcol)
        import org.apache.spark.ml.classification.LogisticRegression
        val lr = new LogisticRegression().setLabelCol("y")//.setFamily("multinomial")  //多项式逻辑回归
        import org.apache.spark.ml.Pipeline
        val pipe = new Pipeline().setStages(Array(va, lr))
        val model = pipe.fit(train)

        println("len：",model.stages.length)

        import org.apache.spark.ml.classification.LogisticRegressionModel
        val mlrModel = model.stages(1).asInstanceOf[LogisticRegressionModel]
        println("mlrModel:",mlrModel)
        println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")  //输出系数
        println(s"Multinomial intercepts: ${mlrModel.interceptVector}")     //输出截距

        val test_r = model.transform(test)
//        test_r.select("y", "prediction", "p", "probability", "rawPrediction").show(10, false)  //展示针对测试集的预测结果

        test_r.groupBy("y").count.show
        test_r.where("y=prediction").groupBy("y").count.show

        test.show()
        test_r.show()

        val raw0 = test_r.groupBy("y").count.where("y=0").first.getLong(1)
        val raw1 = test_r.groupBy("y").count.where("y=1").first.getLong(1)

        val pre0 = test_r.where("y=prediction").groupBy("y").count.where("y=0").first.getLong(1)
        val pre1 = test_r.where("y=prediction").groupBy("y").count.where("y=1").first.getLong(1)

        1.0*pre0/raw0 //对未违约类别预测准确率
        println(1.0*pre0/raw0)
        1.0*pre1/raw1 //对违约类别预测准确率
        println(1.0*pre1/raw1)

        val udf_score = udf(com_score _)
//        println(test_r("probability").)

          val score = test_r.withColumn("score", udf_score(test_r("probability")))

         score.select("SIGN_OBJ_ID","score","y", "prediction", "p").show()
         score.select("SIGN_OBJ_ID","score","y", "prediction", "p").write.mode("overwrite").
         format("csv").save("data123.csv")
//                 score.write.mode("overwrite").format("csv").save("data123.csv")
        import spark.implicits._
    }

    import org.apache.spark.sql.Row
    def ilog(i:Double) = {
        math.log(i)
    }
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions._
    //计算IV值
    def IV(df:Dataset[_], y:String, g:String, T:Long, B:Long) = {
        val G = T - B
        val t1 = df.groupBy(g).agg(count("y").as("T"), sum("y").as("B"))

        val t2 = t1.withColumn("G", t1("T")-t1("B"))

        val udf1 = udf(ilog _)
        val r = t2.withColumn("woe", udf1( t2("B")/B*G/t2("G") )).
        withColumn("iv", (t2("B")/B - t2("G")/G) * udf1( t2("B")/B*G/t2("G") ))

        r.groupBy().agg(sum("iv")).first.getDouble(0)
    }
    //计算WoE值
    def woe(df:Dataset[_], y:String, g:String, newCol:String) = {
        val T = df.count
        val B = df.agg(sum("y")).first.getLong(0)  // 违约总数
        val G = T - B  // 不违约总数
        val t1 = df.groupBy(g).agg(count("y").as("T"), sum("y").as("B")) //T 每组总数  B 每组违约数
        val t2 = t1.withColumn("G", t1("T")-t1("B")) // G 每组不违约数

        val udf2 = udf(ilog _)
        val r = t2.withColumn(newCol, udf2( t2("B")/B*G/t2("G") )).select(g, newCol)
        val r1 = t2.withColumn(newCol, udf2( t2("B")/B*G/t2("G") )).select(col(g).cast(StringType), col(newCol))
          .collect()
          .map(r => (r.getString(0), r.getDouble(1)))
          .toMap
        df.join(r, g)
    }

    //算分
    import org.apache.spark.ml.linalg.Vector
    def com_score(r:Row) = {
        val p = r.toSeq(0).asInstanceOf[Vector](1)
        println(p)
        val score = 304.7 - 75.5*math.log(p/(1-p))  //对应公式，待调整
        score
    }
    def com_score1(pv:Vector) = {
      val p = pv(0)
      304.7 - 75.5*math.log(p/(1-p))  //对应公式，待调整
    }

}
