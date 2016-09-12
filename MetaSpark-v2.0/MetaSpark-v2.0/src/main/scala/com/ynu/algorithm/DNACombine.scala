package com.ynu.algorithm

import scala.io.Source
import java.io._

/**
 * Extract data from 
 * ftp://ftp.ncbi.nlm.nih.gov/refseq/release/bacteria/
 * (1) download  some *.genomic.fna.gz file, unzip to a folder
 * (2) the original file looks like
 * >gi|546540813|ref|NZ_CBML010000001.1| Clostridium chauvoei JF4335 WGS project CBML000000000 data, contig 00016, whole genome shotgun sequence
 * TCTTATCGCCTCAAGCGACTTGTTTATCTTACCACCTTAAGAATTTCTTGTCAACATATTTTTTAATATGTTTTAAAGAG
 * TTTTCCGAGTCATTTGCGACCCGTCTCTCAAGGACAAGGACTATTCTATCATAGCATTTAATATATACATATATCTTTGC
 * TCTATTCTAATCAAATTATTCTTATTTTTCTACAATTATACTGATTTTTCCTTGATTTTTCTTATATTGATACACTAGGT
 * AAAGTTTAAGAAATTACTTATTCAATTAAAATACATTTATGTATTTTAATTGATAATAGTTCTATGAAAAATAACTAAAT
 * TACTTTTTATAGAACTATTTTACTTAGATAAAATATCAAAATAAATGTTACTTCTATGTAGTCTTAATAAAAAGTTTCAA
 * 
 *     using this programming we can get a file looks like
 * NZ_CBML010000001.1 TCTTATCGCCTCAAGCGACTTGTTTATCTTACCACCTTAAGAATTTCTTGTCAACATATTTTTTAATATGTTTTAAAGAGTTTTCCGAGTCATTTGCGACCCGTCTCTCAAGGACAAGGACTATTCTATCATAGCATTTAATATATACATATATCTTTGCTCTATTCTAATCAAATTATTCTTATTTTTCTACAATTATACTGATTTTTCCTTGATTTTTCTTATATTGATACACTAGGTAAAGTTTAAGAAATTACTTATTCAATTAAAATACATTTATGTATTTTAATTGATAATAGTTCTATGAAAAATAACTAAATTACTTTTTATAGAACTATTTTACTTAGATAAAATATCAAAATAAATGTTACTTCTATGTAGTCTTAATAAAAAGTTTCAAACTAATTAGTTATCAATTAAATAAAAATCAATATAGAAATATTTTCAATAAGGTATTATAATATATTAAGTATAATTTGCATCTATAGCTCAGTTGGATAGAGTGCTGGACTTCGAATCCAGGCGTCGGGGGTTCGAATCCCTCTAGGTGCACCAAAAAAGAGTATATATCAGAATCTGATATATACTCTTTTTCTATAATAATAACTTTTGTTCTTTAAGAACATATCTTAAGAGAATTATAAATGAAAATAAAATAAAGTTATTTTGAAAAATAAAAAATGTAGCACTAACAACTTTATTTTTTTCATCTAATAAATATTCAGTTGGCCAATATGACATAATATTAACTATTAAATAACTTCCAATAATTATTTTCAATATAATATTCATATTAGATAAATATACTGTTGAATAAGCTCCAAAACTTAGCACAATAGCAAATATTAAATTTAATATTCGTATAAATATATTAGATCTTTCGATATCTTGCCAGTAAAACATTCTAAAAATACCTTTTGGCGTTATAATTAATTTTCCCATAACTCACTACCTCAACATTGTATTTTACTTTACTAATACACATGCAGAAATTGCTTCGACTATTAAGCTGTTCCCTTCTATATTATTTAACTCTTTTGTTCCTGCAAAATCTTTATTCACAATTATCTTCCATCTATCATTAGA
 * 
 * (3) the result would store in *.combine file under the same folder
 * 
 */
object DNACombine extends App {

  val shuffCount = 10

  /**
   * get all subfiles in a folder
   */
  def subFiles(dir: File): Iterator[File] = {
    val folderChildren = dir.listFiles.filter(_.isDirectory)
    val fileChildren = dir.listFiles.filter(!_.isDirectory)
    fileChildren.toIterator ++ folderChildren.toIterator.flatMap(subFiles _)
  }

  /**
   * extract data from original file
   */
  def combineDNA(refFile: File) = {
    val bacteriaFilePath = refFile.getPath
    val source = Source.fromFile(bacteriaFilePath)
    val lineIterator = source.getLines()

    val lineMap = lineIterator.toList
    val lineIndexList = lineMap.zipWithIndex.toList

    val nameIndexList = lineIndexList.filter(x => x._1.contains(">gi"))

    // generate a new file with a suffix of combine in the same folder  
    val writeFilePath = bacteriaFilePath + ".combine"
    val writer = new PrintWriter(new File(writeFilePath), "UTF-8")
    val currntTime = System.currentTimeMillis()

    for (i <- 0 until nameIndexList.size) {
      val currentNameT = nameIndexList(i)
      val currentName = currentNameT._1.split('|')(3)
      val currentIndex = currentNameT._2

      var nextIndex = 0
      if (i == nameIndexList.size - 1) {
        nextIndex = lineIndexList.size
      } else {
        nextIndex = nameIndexList(i + 1)._2
      }

      val DNA_Sequence = lineIndexList.slice(currentIndex + 1, nextIndex)
      val DNA_str = new StringBuffer
      for (i <- 0 until DNA_Sequence.size) {
        DNA_str.append(DNA_Sequence(i)._1.replaceAll("[^ATGCNWatgcnw]", "N"))
      }

      writer.write(currentName + " " + DNA_str + "\n")
    }

    writer.close()

    println(refFile.getName + ": used time: " + (System.currentTimeMillis() - currntTime))
  }

  // you should replace the bacteria data folder
  val allFiles = subFiles(new File("D:\\UserDocument\\Downloads\\bacteria\\bacteria"))
  allFiles.foreach {
    combineDNA(_)
  }
}
