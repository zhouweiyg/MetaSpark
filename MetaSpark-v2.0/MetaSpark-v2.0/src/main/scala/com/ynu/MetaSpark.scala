/*
 * MetaSpark.scala for MetaSpark
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

import com.ynu.algorithm.AlgorithmBase
import com.ynu.entity.{MetaSparkEntity, Read, Reference}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer
import scala.math.abs

/**
 * @author hadoop
 *
 * MetaSpark
 *
 */
object MetaSpark {

  def main(args: Array[String]) {

    /**
     * --read     <string>   reads file
     * --ref      <string>   reference genome sequences file
     * --refindex <string>   reference genome sequences index file
     * --result   <string>   output recruitments file
     *
     * --identity <int>      sequence identity threshold(%), default=75 (-c)
     * --aligment <int>      minimal alignment coverage control for the read (g=0), default=30 (-m)
     * --evalue   <double>   e-value cutoff, default=10 (-e)
     * --kmersize <int>      k-mer size (8<=k<=12), default=11 (-k)
     * --overlap  <int>      k-mer overlap of index (1<=p<-k), using small overlap for longer reads(454, Sanger), default=8 (-p)
     */
    if (args.length < 3) {
      System.err.println("-----ERROR:Usage --read readFilePath --refFilePath --resultFilePath")
      System.exit(1)
    }

    // parase the params
    val readFilePath = if (args.indexOf("--read") > -1) args(args.indexOf("--read") + 1).trim else ""
    val refFilePath = if (args.indexOf("--ref") > -1) args(args.indexOf("--ref") + 1).trim else ""
    val resultFilePath = if (args.indexOf("--result") > -1) args(args.indexOf("--result") + 1).trim else ""
    val refIndexFilePath = if (args.indexOf("--refindex") > -1) args(args.indexOf("--refindex") + 1).trim else ""

    if (readFilePath.equals("") || refFilePath.equals("") || resultFilePath.equals("")) {
      System.err.println("-----ERROR:Usage --read readFilePath --refFilePath --resultFilePath")
      System.exit(1)
    }

    val SEQUENCE_IDENTITY_THRESHOLD = {
      if (args.indexOf("--identity") > -1) {
        val inputValue = args(args.indexOf("--identity") + 1).toInt
        if (inputValue < 0 || inputValue > 100) 75 else inputValue
      } else 75
    }
    val MINIMAL_ALIGNMENT_COVERAGE = {
      if (args.indexOf("--aligment") > -1) {
        val inputValue = args(args.indexOf("--aligment") + 1).toInt
        if (inputValue < 0) 30 else inputValue
      } else 30
    }
    val E_VALUE_CUTOFF = if (args.indexOf("--evalue") > -1) args(args.indexOf("--evalue") + 1).toDouble else 10
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
      .set("spark.storage.memoryFraction", "0.7")
      .set("spark.shuffle.memoryFraction", "0.1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") ///.setJars(jars)
      .set("spark.rdd.compress", "true")
      .set("spark.kryoserializer.buffer.mb", "256")

    val sparkContext = new SparkContext(conf)

    // if the result folder exist, delete
    // val output = new org.apache.hadoop.fs.Path(resultFilePath);
    // val hdfs = org.apache.hadoop.fs.FileSystem.get(
    //  new java.net.URI("hdfs://hadoop-cluster"), new org.apache.hadoop.conf.Configuration())
 
    // if (hdfs.exists(output)) hdfs.delete(output, true)

    //-------------make ref index ------------------------------------------------------------------
    // reffile
    val refFileRDD = sparkContext.textFile(refFilePath)

    // refRDD
    // (Number, name, ref, length)
    val refBaseRDD: RDD[Reference] = refFileRDD.mapPartitions((y: Iterator[String]) => {
      y.map(x => {
        val tmp: Array[String] = x.split(" ")
        new Reference(tmp(0).toInt, tmp(1), tmp(2), tmp(2).length)
      })
    })

    //------PRINT-------

    // refIndex
    // (KmerString, (refNumber, KmerString Start Positon Index))
    val refIndex: RDD[(String, (Int, Int))] = {
      if (refIndexFilePath.equals("")) {
        // 构建ref index
        refBaseRDD.flatMap(x => {
          val tmp: IndexedSeq[(String, Int)] = AlgorithmBase.indexOne(K_MER_SIZE, K_MER_SIZE - K_MER_OVERLAP, x.getRef)
          val idx: IndexedSeq[(String, (Int, Int))] = tmp.map(y => (y._1, (x.getNo, y._2)))
          idx
        })
      } else {
        // get ref index from hdfs
        sparkContext.textFile(refIndexFilePath).map(x => {
          val splitPoint = x.replace("(", "").replace(")", "").split(',')
          (splitPoint(0).toString, (splitPoint(1).toInt, splitPoint(2).toInt))
        })
      }
    }
    val refRDDBroadcast = sparkContext.broadcast(refBaseRDD.map(x => (x.getNo, (x.getName, x.getRef))).toLocalIterator.toMap)

    val idxGBK: RDD[(String, Iterable[(Int, Int)])] = refIndex.groupByKey()
    val refSumLen = refBaseRDD.map(x => x.getRef.length).reduce(_ + _)
    val total_num = refBaseRDD.count()

    val eHSP: Int = AlgorithmBase.expectedHSPlength(AlgorithmBase.mink, AlgorithmBase.preadLen, refSumLen, AlgorithmBase.Hh)
    val eLengthSeq = AlgorithmBase.effectiveLengthSeq(AlgorithmBase.preadLen, eHSP, AlgorithmBase.mink)
    val eLengthDB = AlgorithmBase.effectiveLengthDb(refSumLen, eHSP, total_num.toInt, AlgorithmBase.mink)
    //-------------make ref index ------------------------------------------------------------------

    //-------------prepare the read rdd ------------------------------------------------------------
    val readFileRDD = sparkContext.textFile(readFilePath)

    val readFileSize = readFileRDD.map(_.length).reduce(_+_)//1000000000
    //println( "_----------------------------------------______---------------------"+readFileSize)
    if(readFileSize>1000000000){// 8988895
        println("The read files are larger than 1G. Continue?(yes/no)")
        val content = Console.readLine()
        if (content!="Y" && content!="y" && content!="yes" && content!="\n") {
        
            println("exit...")
            //System.exit(0)
            sparkContext.stop()
        }
    }

    // --add reverse comlementary read ----
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
    })

    // --add direction info--
    val directionForwardFileRDD = readFileRDD.map(x => x + " " + "+")
    val directionReverseComplementaryFileRDD = reverseComplementaryFileRDD.map(x => x + " " + "-")
    directionReverseComplementaryFileRDD.cache.take(1)
    println ("------init: " + new java.util.Date())

    def FR_Hit_Main(readRDD: RDD[String]): RDD[(String, MetaSparkEntity)] = {

      // (readNumber, readString)
      val readBaseRDD = readRDD.mapPartitions(x => {
        x.map(x => {
          val arr = x.split(" ")
          val rN: String = arr(0)
          val rS: String = arr(1)
          // --add direction info--
          val rDirection: String = arr(2)
          val read = new Read(rN, rS.length, rDirection)
          (read.No, rS)
        })
      })

      val readRDDBroadcast: Broadcast[Map[Int, String]] = sparkContext.broadcast(readBaseRDD.toLocalIterator.toMap)

      val readListRDD: RDD[(Read, String, Int)] = readRDD.repartition(138).flatMap(x => {
        val arr = x.split(" ")
        val rN: String = arr(0)
        val rS: String = arr(1)
        // --add direction info--
        val rDirection: String = arr(2)
        val read = new Read(rN, rS.length, rDirection)
        val tmp = for (
          i <- 0 until rS.length - K_MER_SIZE + 1
        ) yield {
          val bin = rS.substring(i, i + K_MER_SIZE)
          (read, bin, i)
        }
        tmp //(Read,Kbin)
      })
      readListRDD.cache.take(1)
      println ("------start forward: " + new java.util.Date())

      // idxGBK: RDD[(String, Iterable[(Int, Int)])]
      // readList: (Read, String, Int) : (1	>1_lane2_1	75	-	,AAATATTTAGT,0)
      // （read line number， Map(Ref line number，<the location read mapped on ref>)，direction）
      val Ahit: RDD[(Int, Map[Int, Stream[Int]], String)] = readListRDD
        .keyBy(x => x._2)
        .join(idxGBK) // (String, ((Read, String, Int), Iterable[(Int, Int)]))
        .mapPartitions(x => {
          x.map(x => {
            val loc: Iterable[(Int, Int)] = x._2._2.map(y => {
              (y._1, y._2 - x._2._1._3)
            })
            // --add direction info--
            (x._2._1._1.No, loc.groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).toStream)), x._2._1._1.readDirection)
          })
        }) //.keyBy(x => x._1.No).groupByKey(numPartitions = 24).map(x => (x._1, x._2.map(x => x._2).reduce(_++_)))

      // (Int, Map[Int, Stream[(Int, Int)]], String)
      // (read line number， Map(Ref line number，<mapped location>)，direction)
      val Hitcans = Ahit.mapPartitions(x => {
        x.map(x => {
          val hit = for {
            i <- x._2
          } yield {
            var hit = new ArrayBuffer[(Int, Int)]()
            val tmp = i._2.sorted
            for (j <- 0 until tmp.length) {
              val refLoc = tmp(j)
              val begin = if (refLoc - K_MER_SIZE > 0) refLoc - K_MER_SIZE else 0
              val end = refLoc + 75 + 2 * K_MER_SIZE
              hit += ((begin, end)) // (read,(id,(start,end)))
            }
            (i._1, hit.toStream)
          }

          // --add direction info--
          // （read line number， （），direction）
          (x._1, hit, x._3)
        })
      }) //.groupByKey().map(x => (x._1, x._2.toArray))

      // Hitcans: (Int, Map[Int, Stream[(Int, Int)]], String)
      // (read line number， Map(Ref line number，<mapped location>)，direction)
      val perfectHit = Hitcans.mapPartitions(x => {
        x.map(x => {
          val phit = x._2.map(x => {
            val x2 = x._2.sorted // mapped location set:Stream[(Int, Int)]
            var result = new ArrayBuffer[(Int, Int)]()
            if (x2.length > 1) {
              var hitcan = x2(0)  
              for (i <- 0 until x2.length) {
                if (hitcan._2 < x2(i)._1) {  
                  result += hitcan
                  hitcan = x2(i)
                } else {  
                  if (hitcan._2 - hitcan._1 < AlgorithmBase.MAX_READ) {
                    hitcan = (hitcan._1, x2(i)._2)
                  }
                }
              }
              result += hitcan
            } else if (x2.length == 1) {
              result += x2(0)
            }
            (x._1, result.toStream)
          })
          // --add direction info--
          (x._1, phit, x._3)
        })
      })

      // perfectHit：(read line number， Map(Ref line number，<mapped location>)，direction)
      val open_phit = perfectHit.flatMap(x => {
        val l1 = x._2.flatMap(y => {
          val l2 = y._2.map(z => {
            // --add direction info--
            (x._1, y._1, z._1, z._2, x._3) // read, refid, start, end, direction
          })
          l2
        })
        l1
      })
      open_phit.cache.take(1)
      println ("------read end: " + new java.util.Date())

      val ali = open_phit.mapPartitions(x => {
        // (Number, name, ref, length)
        val refS = refRDDBroadcast.value

        x.map(x => {
          val tmp = refS(x._2)
          val len = tmp._2.length
          val realEnd = if (x._4 > len - 1) len - 1 else x._4

          // --add direction info--
          //(x._1, tmp._1, x._3, x._4, tmp._2.substring(x._3, realEnd), x._5)
          (x._1, tmp._1, x._3, x._4, tmp._2.substring(x._3, realEnd), x._5)
          // readid, refname, start, end, mapping string on ref, direction
        })
      })

      val align = ali.mapPartitions(x => {
        // (readNumber, readString)
        val rS = readRDDBroadcast.value
        x.map(x => {
          // --add direction info--
          (x._1, x._2, x._3, x._4, x._5, rS(x._1), x._6)
          // readid, refid, start, end, ref, read, direction
        })
      })

      val alignment = align.mapPartitions(x => {

        x.map(x => {
          val readName = x._1
          val refName = x._2
          val start = x._3.toInt
          val end = x._4.toInt
          try {
            val refStr = x._5.map(x => {
              if ("ATGCNWatgcnw".contains(x)) AlgorithmBase.alphabet(x) else 0
            })
            val readStr = x._6.map(x => {
              if ("ATGCNWatgcnw".contains(x)) AlgorithmBase.alphabet(x) else 0
            })
            // --add direction info--
            val direction = x._7

            val wp = AlgorithmBase.CreateFourmers(readStr)
            val result = AlgorithmBase.diag_test_aapn_est_circular(refStr, readStr.length, wp)
            (readStr, refStr, refName, start, end, readName, result, direction)
          } catch {
            case t: Throwable => {
              System.err.println("-----ERROR: " + t.getLocalizedMessage)
              (null, null, refName, start, end, readName, (0, 0, 0), "+")
            }
          }
        }).filter(x => x._7._3 >= AlgorithmBase.BEST_KMERS).map(x => {
          val tmp = AlgorithmBase.local_band_align2(x._1, x._2, x._7._1, x._7._2)
          val eValue = AlgorithmBase.rawScoreToExpect(tmp._4, AlgorithmBase.mink, AlgorithmBase.lamda, eLengthSeq, eLengthDB)
          val read_cov = abs(tmp._1(2) - tmp._1(1)) + 1
          val read_identity_d = (tmp._2 * 100).toDouble / tmp._3.toDouble

         
          val reverse_read_begin = if (x._8.equals("-")) (math.abs(tmp._1(1) + 1 - AlgorithmBase.preadLen) + 1) else tmp._1(1) + 1
          val reverse_read_end = if (x._8.equals("-")) (math.abs(tmp._1(2) + 1 - AlgorithmBase.preadLen) + 1) else tmp._1(2) + 1

           
          // (ReadName, ReadLength, E-value, ReadBegin, ReadEnd, Strand, Identity, ReferenceSequenceName, RefBegin, RefEnd)
          val finalResult = MetaSparkEntity(x._6.toInt, AlgorithmBase.ReadLength, eValue.toString, AlgorithmBase.preadLen,
            reverse_read_begin, reverse_read_end, x._8, read_identity_d.toString,
            x._3, (x._4 + tmp._1(3) + 1), (x._4 + tmp._1(4) + 1))
          (finalResult, read_cov)
        }).filter(x => x._1.Identity.toDouble >= SEQUENCE_IDENTITY_THRESHOLD && x._2 >= MINIMAL_ALIGNMENT_COVERAGE && x._1.E_Value.toDouble <= E_VALUE_CUTOFF)
          .map(x => {
            (x._1.ReadName + "_" + x._1.RefBegin + "_" + x._1.RefEnd + "_" + x._1.Strand, x._1)
          })
      })
      alignment
    }
    val forwardAlignmentRDD = FR_Hit_Main(directionForwardFileRDD).cache
    forwardAlignmentRDD.take(1)
    println ("------forward end: " + new java.util.Date())
    val reverseComplementaryAlignmentRDD = FR_Hit_Main(directionReverseComplementaryFileRDD).cache
    reverseComplementaryAlignmentRDD.take(1)
    println ("------reverse end: " + new java.util.Date())

    val allAlignmentRDD = forwardAlignmentRDD.union(reverseComplementaryAlignmentRDD)

    allAlignmentRDD.reduceByKey((x, y) => x).map(_._2).sortBy(x => x.ReadName, true, 1).saveAsTextFile(resultFilePath)

    sparkContext.stop()
  }
}
