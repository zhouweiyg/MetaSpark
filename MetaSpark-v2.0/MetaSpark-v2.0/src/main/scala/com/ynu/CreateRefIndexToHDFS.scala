/*
 * CreateRefIndexToHDFS.scala for MetaSpark
 * Copyright (c) 2015-2016 Wei Zhou, Changchun Liu, Shuo Yuan All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
 
package com.ynu

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.immutable.IndexedSeq
import com.ynu.entity.Reference
import com.ynu.algorithm.AlgorithmBase

/**
 * Created by hadoop on 15-8-5.
 * create reference index
 */
object CreateRefIndexToHDFS {

  def main(args: Array[String]): Unit = {
    /**
     * --ref        <string>   reference genome sequences file
     * --result     <string>   result file
     * 
     * --kmersize   <int>      k-mer size (8<=k<=12), default=11
     * --overlap    <int>      k-mer overlap of index (1<=p<-k), using small overlap for longer reads(454, Sanger), default=8 (-p)
     */
    if (args.length < 2) {
      System.err.println("-----ERROR:Usage --refFilePath --resultFilePath----------")
      System.exit(1)
    }

    // parase the params
    val refFilePath = if (args.indexOf("--ref") > -1) args(args.indexOf("--ref") + 1).trim else ""
    val resultFilePath = if (args.indexOf("--result") > -1) args(args.indexOf("--result") + 1).trim else ""

    if (refFilePath.equals("") || resultFilePath.equals("")) {
      System.err.println("-----ERROR:Usage --refFilePath --resultFilePath----------")
      System.exit(1)
    }

    val K_MER_SIZE = {
      if (args.indexOf("--kmersize") > -1) {
        val inputValue = args(args.indexOf("--kmersize") + 1).toInt
        if (inputValue < 8 || inputValue > 12) 12 else inputValue
      } else 11
    }
    val K_MER_OVERLAP = {
      if (args.indexOf("--overlap") > -1) {
        val inputValue = args(args.indexOf("--overlap") + 1).toInt
        if (inputValue < 1 || inputValue >= K_MER_SIZE) 8 else inputValue
      } else 8
    }

    val conf = new SparkConf()
    
    conf.set("spark.default.parallelism", "138")
      .set("spark.akka.frameSize", "1024")
      .set("spark.storage.memoryFraction", "0.5")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ///.setJars(jars)
      .set("spark.rdd.compress", "true")
      .set("spark.kryoserializer.buffer.mb", "256")

    val sparkContext = new SparkContext(conf)

     // if the result folder exist, delete
    // val output = new org.apache.hadoop.fs.Path(resultFilePath);
    // val hdfs = org.apache.hadoop.fs.FileSystem.get(
    //   new java.net.URI("hdfs://hadoop-cluster"), new org.apache.hadoop.conf.Configuration())

 
    // if (hdfs.exists(output)) hdfs.delete(output, true)
    
    //-------------make ref index ------------------------------------------------------------------
    // reffile
    val refFile = sparkContext.textFile(refFilePath)
    // println("--------------refFileCount: " + refFile.count() + "-----------------")

    // refRDD
    val refRDD: RDD[Reference] = refFile.mapPartitions((y: Iterator[String]) => {
      y.map(x => {
        val tmp: Array[String] = x.split(" ")
        new Reference(tmp(0).toInt, tmp(1), tmp(2), tmp(2).length)
      })
    })
    //println("--------------RefCount: " + Ref.count() + "-----------------")

    // binref
    val refIndex: RDD[(String, (Int, Int))] = refRDD.flatMap(x => {
      val tmp: IndexedSeq[(String, Int)] = AlgorithmBase.indexOne(K_MER_SIZE, K_MER_SIZE - K_MER_OVERLAP, x.getRef)
      val idx: IndexedSeq[(String, (Int, Int))] = tmp.map(y => (y._1, (x.getNo, y._2)))
      idx
    })

    refIndex.saveAsTextFile(resultFilePath)
    sparkContext.stop()
    //-------------make ref index ------------------------------------------------------------------
  }

}
