package com.ynu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BacteriaCleanTool {
  def main(args: Array[String]): Unit = {
    /**
     * --input        <string>   bacteria file
     * --result       <string>   result file
     */
    if (args.length < 2) {
      System.err.println("-----ERROR:Usage --input inputFilePath --result resultFilePath")
      System.exit(1)
    }

    // parase the params
    val inputFilePath = if (args.indexOf("--input") > -1) args(args.indexOf("--input") + 1).trim else ""
    val resultFilePath = if (args.indexOf("--result") > -1) args(args.indexOf("--result") + 1).trim else ""

    if (inputFilePath.equals("") || resultFilePath.equals("")) {
      System.err.println("-----ERROR:Usage --input inputFilePath --result resultFilePath")
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

    val bactRDD = sparkContext.textFile(inputFilePath).map(x => x.split(" ")).map(x => (x(0), x(1).replaceAll("[^ATGCNWatgcnw]", "N")))
    
    
    val bactIndexRDD = bactRDD.zipWithIndex
    bactIndexRDD.map(x => (x._2 + 1).toString + " " + x._1._1 + " " + x._1._2).saveAsTextFile(resultFilePath)
    
    sparkContext.stop
  }
}