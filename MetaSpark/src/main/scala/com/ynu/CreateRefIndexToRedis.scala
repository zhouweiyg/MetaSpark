package com.ynu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import redis.clients.jedis.ShardedJedisPool
import com.ynu.common.RedisClientFactory
import com.ynu.common.MetaSparkUtility

/**
 * 存成HashMap
 * RefIndex GroupByKey之后存到Redis，每个Key对应很多坐标
 *
 * ATFGGG => {123-1212, 213-2343, 3434-2323}
 * ATFGGG => {112-1452, 223-2365, 3567-2343}
 *
 */
object CreateRefIndexToRedis {
  
  def main(args: Array[String]): Unit = {
    /**
     * --refindex    <string>   reads genome sequences file
     */
    if (args.length < 1) {
      System.err.println("-----ERROR:Usage --refindex")
      System.exit(1)
    }

    // parase the params
    val refIndexFilePath = if (args.indexOf("--refindex") > -1) args(args.indexOf("--refindex") + 1).trim else ""

    if (refIndexFilePath.equals("")) {
      System.err.println("-----ERROR:Usage --refindex")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.set("spark.default.parallelism", "138")
      .set("spark.akka.frameSize", "1024")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.kryoserializer.buffer.mb", "256")

    val sc = new SparkContext(conf)

    val refIndexRDD = sc.textFile(refIndexFilePath).map(x => {
      val splitPoint = x.replace("(", "").replace(")", "").split(',')
      (splitPoint(0).toString, (splitPoint(1).toInt, splitPoint(2).toInt))
    })

    val idxGBK: RDD[(String, Iterable[(Int, Int)])] = refIndexRDD.groupByKey()

    val tmpIdxResult = idxGBK.mapPartitions(idxPartition => {
      // 内部类，初始化连接池，程序退出时自动销毁
      object InternalRedisClient extends Serializable {
        @transient private var jedisShardedPool: ShardedJedisPool = null

        def makePool(): Unit = {
          if (jedisShardedPool == null) {
            jedisShardedPool = new ShardedJedisPool(RedisClientFactory.getPoolConfig, RedisClientFactory.shardsServerHostList)

            val hook = new Thread {
              override def run = jedisShardedPool.destroy()
            }
            sys.addShutdownHook(hook.run)
          }
        }

        def getPool: ShardedJedisPool = {
          assert(jedisShardedPool != null)
          jedisShardedPool
        }
      }

      InternalRedisClient.makePool
      val jedisShardedPool = InternalRedisClient.getPool

      idxPartition.map(x => {
        val refString = x._1
        val refPosition = x._2

        var positionMap = new java.util.HashMap[String, String]()

        // 因为Kmer在一个ref中可能有很多个位置，所以Redis设置Key不能仅仅以ref number，这里设置refNumber-position为Key
         // [ref-position:""]
        refPosition.foreach(x => {
          positionMap.put(x._1.toString + "-" + x._2.toString, "")
        })

        if (positionMap.size() > 0) {
          val sentinelConnection = jedisShardedPool.getResource
          sentinelConnection.hmset(MetaSparkUtility.REDIS_REFINDEX_HS_PREFIX + ":" + refString, positionMap)
          sentinelConnection.close
        }
      })
    })

    println("--------refIndex:" + tmpIdxResult.count)

    sc.stop()
  }
}