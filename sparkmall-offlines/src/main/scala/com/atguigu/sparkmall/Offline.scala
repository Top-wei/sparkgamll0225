package com.atguigu.sparkmall


import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.Requirements.{SessionTop10, Top10}

import com.atguigu.sparkmall.util.Condition
import com.atguigu.sparkmall.util.bean.DataModel.UserVisitAction
import common.ConfigurationUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession






/**
  * @-auther shangshengwei
  * @-create 2019-07-18  15:07
  */
object Offline {


  def main (args: Array[ String ]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("OfflineApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val taskId :String= UUID.randomUUID().toString

    val userActionRdd: RDD[UserVisitAction] = readUserActionRDD(spark, readCondition)
    //放到hdfs上
   /* spark.sparkContext.setCheckpointDir("hdfs://hadoop201:9000/spark0225")
    userActionRdd.cache()
    userActionRdd.checkpoint()*/
    // userActionRdd.take(10).foreach(println)
  // println (userActionRdd.count ( ))
    //需求一
    val infoes = Top10.statCategoryTop10(spark, userActionRdd,taskId)

    SessionTop10.statCategoryTop10Session(spark:SparkSession,infoes,userActionRdd, taskId)




  }
  //从用户行为表读取Rdd,和用户表关联需要年龄过滤，1=1防止第一个不走就直接and，start为空
  def readUserActionRDD(spark: SparkSession,condition:Condition) ={
    var sql =
      s"""
        |select
        | v.*
        |from user_visit_action v join user_info u on v.user_id=u.user_id
        |where 1=1
      """.stripMargin
    if (isNotEmpty(condition.startDate)) {
      sql += s" and date>='${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and date<='${condition.endDate}'"
    }

    if (condition.startAge > 0) {
            sql += s" and u.age>=${condition.startAge}"
        }
    if (condition.endAge > 0) {
            sql += s" and u.age<=${condition.endAge}"
        }


    import spark.implicits._
    spark.sql("use ojbk")
    spark.sql(sql).as[UserVisitAction].rdd
  }

  //读取sesion文件需要的字段
  def readCondition: Condition = {
    val conditionString: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(conditionString, classOf[Condition])
    //解析json字符串
  }

}


