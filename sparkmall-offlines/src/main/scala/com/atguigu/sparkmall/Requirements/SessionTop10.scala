package com.atguigu.sparkmall.Requirements

import java.util.Properties

import com.atguigu.sparkmall.bean.{CategoryCountInfo, CategorySession}
import com.atguigu.sparkmall.util.bean.DataModel.UserVisitAction
import org.apache.hadoop.mapreduce.v2.api.records.TaskId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @-auther shangshengwei
  * @-create 2019-07-19  11:44
  */
object SessionTop10 {
  //根据top10的品类把session找出来，统计数量，最后排序，取出前10，数据是10*10，写入mysql
  def  statCategoryTop10Session(spark:SparkSession, a:List[CategoryCountInfo], userActionRdd:RDD[UserVisitAction], taskId: String) = {

    val categoryIdList: List[ String ] = a.map (_.categoryId)
    val FuvaRDD = userActionRdd.filter (uva => {
      if (uva.click_category_id != -1) {
        categoryIdList.contains (uva.click_category_id.toString) // 判断点击action是否在top10中
      } else if (uva.order_category_ids != null) {
        val cids: Array[ String ] = uva.order_category_ids.split (",")
        categoryIdList.intersect (cids).nonEmpty // 判断下单行为是否在top10中

      } else if (uva.pay_category_ids != null) {
        val cids = uva.pay_category_ids.split (",")
        categoryIdList.intersect (cids).nonEmpty // 判断支付行为是否在top10中
      } else {
        false
      }
    })
    val categorySessionOne = FuvaRDD.flatMap (uva => {
      //uva  有可能是点击, 也可能是下单, 支付
      if (uva.click_category_id != -1) {
        Array (((uva.click_category_id + "", uva.session_id), 1))
      } else if (uva.order_category_ids != null) {
        val cids: Array[ String ] = uva.order_category_ids.split (",")
        categoryIdList.intersect (cids).map (cid => {
          ((cid, uva.session_id), 1)
        })
      } else {
        val cids: Array[ String ] = uva.pay_category_ids.split (",")
        categoryIdList.intersect (cids).map (cid => {
          ((cid, uva.session_id), 1)
        })
      }
    })
    //数据类型改变
    val cateSessionCount = categorySessionOne.reduceByKey (_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }
    //聚合排序取出前10
    val categorySessionTop10 = cateSessionCount.groupByKey ( ).map {
      case (cid, sessionCounyIt) => {
        (cid, sessionCounyIt.toList.sortBy (_._2)(Ordering.Int.reverse).take (10))
      }
    }

    val resultRdd: RDD[CategorySession] = categorySessionTop10.flatMap {
      case (cid, sessionCountList) => {
        sessionCountList.map {
          case (sid, count) => CategorySession( taskId.toString, cid, sid, count)
        }
      }
    }
    import spark.implicits._

    val prop = new Properties ( )
    prop.setProperty ("user", "root")
    prop.setProperty ("password", "000000")
    resultRdd.toDF ( ).write.mode ( SaveMode.Overwrite).jdbc ("jdbc:mysql://hadoop201:3306/sparkmall", "category_top10_session", prop)


  }


}
