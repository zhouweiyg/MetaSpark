package com.ynu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.JedisSentinelPool
import com.ynu.common.MetaSparkUtility
import com.ynu.common.RedisClientFactory

/**
 * Reads内存数据库操作，包括
 * 1 create memerydb for inputpath file
 * 2 delete memerydb for inputpath file
 */
object CreateRedisReadHS {

  def main(args: Array[String]): Unit = {
    /**
     * --read        <string>   reads genome sequences file
     * --opt         <string>   redis operate,create or delete
     */
    if (args.length < 2) {
      System.err.println("Usage --read --opt(create,delete)")
      System.exit(1)
    }

    // parase the params
    val readFilePath = if (args.indexOf("--read") > -1) args(args.indexOf("--read") + 1).trim else ""
    val operation = if (args.indexOf("--opt") > -1) args(args.indexOf("--opt") + 1).trim else ""

    if (readFilePath.equals("") || operation.equals("")) {
      System.err.println("Usage --read --opt(create,delete)")
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

    val sparkContext = new SparkContext(conf)

    operation match {
      case MetaSparkUtility.OPT_CREATE => createReadRedisHS(readFilePath)
      case MetaSparkUtility.OPT_DELETE => deleteRedisHS(readFilePath)
    }

    sparkContext.stop()
    
    def createReadRedisHS(filePath: String) = {
      val fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."))
      val redisHSName = MetaSparkUtility.getRedisHSName(MetaSparkUtility.FILE_READ, fileName)

      println("------Create Redis For " + redisHSName + "-----------")
      
      //-------------prepare the read rdd ------------------------------------------------------------
      val readFileRDD = sparkContext.textFile(filePath).map(x => x + " " + "+")

      // --add reverse comlementary read 反向互补----
      val reverseComplementaryFileRDD = readFileRDD.map(x => {
        // reverse and pair
        val arr = x.split(" ")
        val rN: String = arr(0)
        val rS: String = arr(1)

        val reverseRS = rS.reverse
        val reversePairBuild = new StringBuilder
        for (i <- 0 to (reverseRS.length - 1)) {
          reverseRS(i).toLower match {
            case 'a' => reversePairBuild.append("T");
            case 't' => reversePairBuild.append("A");
            case 'g' => reversePairBuild.append("C");
            case 'c' => reversePairBuild.append("G");
            case 'n' => reversePairBuild.append("N");
          }
        }
        (rN + " " + reversePairBuild.toString())
      }).map(x => x + " " + "-")

      println("---------------Build RDD over--------------")
      
      val totalRDD = readFileRDD.union(reverseComplementaryFileRDD)
      println("---------------totalRDD Count:" + totalRDD.count())
      
      val resultRDD = totalRDD.mapPartitions(x => {

        val jedisSentinelPool = new JedisSentinelPool("mymaster", RedisClientFactory.sentinelServerHostHS, RedisClientFactory.getPoolConfig, RedisClientFactory.TIMEOUT)
        val sentinelConnection = jedisSentinelPool.getResource

        val result = x.map(x => {
          val arr = x.split(" ")
          val rN: String = arr(0)
          val rS: String = arr(1)
          // --add direction info--
          val rDirection: String = arr(2)

          sentinelConnection.hset(redisHSName, rDirection + rN.split("_")(2), rS)
          println("-----------Insert "  + rDirection + rN + "----------")
        })

        sentinelConnection.close()
        jedisSentinelPool.destroy()

        result
      })
      println("---------------resultRDD Count:" + resultRDD.count())
    }

    def deleteRedisHS(filePath: String) = {
      val fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."))
      val redisHSName = MetaSparkUtility.getRedisHSName(MetaSparkUtility.FILE_READ, fileName)

      val jedisSentinelPool = new JedisSentinelPool("mymaster", RedisClientFactory.sentinelServerHostHS, RedisClientFactory.getPoolConfig, RedisClientFactory.TIMEOUT)
      val sentinelConnection = jedisSentinelPool.getResource
      sentinelConnection.del(redisHSName)

      sentinelConnection.close()
      jedisSentinelPool.destroy()
    }
  }
}