package com.atguigu.sparkmall.Requirements

import java.util.Properties

import com.atguigu.sparkmall.acc.accTest
import com.atguigu.sparkmall.bean
import com.atguigu.sparkmall.bean.CategoryCountInfo
import com.atguigu.sparkmall.util.bean.DataModel.UserVisitAction
import com.atguigu.sparkmall.util.bean.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @-auther shangshengwei
  * @-create 2019-07-18  15:22
  */
object Top10 {

  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction],taskId:String) = {

    val acc = new accTest
    spark.sparkContext.register(acc)
    userVisitActionRDD.foreach(a => {
      acc.add(a)
    })
    val sortedList = acc.value.toList.sortBy{
      case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
    }.take(10)
    println (sortedList)

    val CategoryCountTop10:List[CategoryCountInfo] = sortedList.map {
      case (cid, (c1, c2, c3)) => bean.CategoryCountInfo(taskId, cid, c1, c2 ,c3)
    }
//无顺序的一次性全部执行
/*    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "000000")
    import spark.implicits._
    Top10CategoryCountInfo.toDF.write.jdbc("jdbc:mysql://hadoop201:3306/sparkmall", "category_top100", props)*/
//拿一个走一个
    val sql ="insert into category_top10 values(?, ?, ?, ?, ?)"
    val args: List[Array[Any]] = CategoryCountTop10.map(cci => {
      Array[Any](cci.taskId, cci.categoryId, cci.clickCount,cci.orderCount, cci.payCount)
    })


    JDBCUtil.executeBatchUpdate(sql, args)


    CategoryCountTop10
  }
}
