package com.atguigu.realtime


import com.atguigu.realtime.app.BlackListApp
import com.atguigu.realtime.bean.AdsInfo
import common.MyKafkaUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @-auther shangshengwei
  * @-create 2019-07-20  11:34
  */
object RealTimeApp {
  def main (args: Array[ String ]): Unit = {
        val conf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")
        val sc = new SparkContext(conf)
        //从kafka获取数据得到DStream
        val scc = new StreamingContext(sc,Seconds(3))
        val recordDStream = MyKafkaUtil.getDStream(scc,topic = "ads_log0225")
          //sc.setCheckpointDir("hdfs：//hadoop201:9000/spark0225")

    val adsInfo = recordDStream.map ( record => {
      val msg = record.value ( )
      val arg = msg.split ( "," )
      //封装到样例类中
      AdsInfo ( arg ( 0 ).toLong, arg ( 1 ), arg ( 2 ), arg ( 3 ), arg ( 4 ) )
    } )


          //需求一
          val filteredAdsInfoDSteam: DStream[AdsInfo] = BlackListApp.filterBlackList(scc, adsInfo)
              BlackListApp.checkUserToBlackList(scc,filteredAdsInfoDSteam)
          //



          scc.stop()
          scc.awaitTermination()



  }
}
