package com.atguigu.realtime.app

import java.util

import com.atguigu.realtime.bean.AdsInfo
import common.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * @-auther shangshengwei
  * @-create 2019-07-20  14:21
  */
object BlackListApp {
  val dayUserAdsCount = "day:userId:adsId"
  val blackList = "blacklist"

  // 检测用户是否要加入到黑名单
  def checkUserToBlackList(scc: StreamingContext, adsInfoDStream: DStream[AdsInfo]) = {
    // redis连接0 ×
    adsInfoDStream.foreachRDD(rdd => {
      // redis连接1  ×
      rdd.foreachPartition(adsInfoIt => {
        // redis连接2 √
        //1.统计每天每用户每广告的点击量
        val client: Jedis = RedisUtil.getJedisClient
        adsInfoIt.foreach(adsInfo => {
          println(adsInfo.userId)
          val field = s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}"
          // 返回值就是增加后的值
          val clickCount = client.hincrBy(dayUserAdsCount, field, 1)
          //2. 加入黑名单
          //                    val clickCount: String = client.hget(dayUserAdsCount, field)
          if (clickCount.toLong > 10) {
            client.sadd(blackList, adsInfo.userId)
          }
        })

        client.close() // 不是真正的关闭连接, 而是把这个连接交个连接池管理
      })
    })
  }

  // 过滤黑名单
  def filterBlackList(scc: StreamingContext, adsInfoDStream: DStream[AdsInfo]) = {

    // 黑名单在实时更新, 所以获取黑名单也应该实时的获取, 一个周期获取一次黑名单
    adsInfoDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val blackBC: Broadcast[util.Set[String]] = scc.sparkContext.broadcast(client.smembers(blackList))
      client.close()
      rdd.filter(adsInfo => {
        val blackListIds: util.Set[String] = blackBC.value
        !blackListIds.contains(adsInfo.userId)
      })
    })

  }
}
