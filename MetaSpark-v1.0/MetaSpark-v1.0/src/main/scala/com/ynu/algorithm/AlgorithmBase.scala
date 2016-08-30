/*
 * AlgorithmBase.scala for MetaSpark
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
 
package com.ynu.algorithm

import com.ynu.entity.WorkingPara

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.math.exp
import scala.collection.immutable.IndexedSeq

object AlgorithmBase extends Serializable {

  val NAA4 = 256
  val NAA3 = 64
  val NAA2 = 16
  val NAA1 = 4
  val BEST_NAS = 24
  val BEST_KMERS = 20
  val band_width = 4
  val MAX_READ = 4000
  val MAX_GAP = 4096
  val MAX_AA = 23
  val MAX_NA = 6
  val mink = 0.621
  val lamda = 1.33
  val Hh = 1.12
  val preadLen = 75

  val ReadLength = "75nt"

  val BLOSUM62_na = Array[Int](

    1, // A
    -2, 1, // C
    -2, -2, 1, // G
    -2, -2, -2, 1, // T
    -2, -2, -2, 1, 1, // U
    -2, -2, -2, -2, -2, 1 // N
    //A  C  G  T  U  N
    //0  1  2  3  3  4
    )

  val alphabet = Map(
    /**
     * 编码转换表，用于将4种基因序列字符(A,C,T,G)映射为Int型数，本质上即对其
     * 进行了二进制编码
     */
    'A' -> 0,
    'a' -> 0,
    'C' -> 1,
    'c' -> 1,
    'G' -> 2,
    'g' -> 2,
    'T' -> 3,
    't' -> 3,
    'N' -> 0,
    'n' -> 0,
    'W' -> 0,
    'w' -> 0)

  /**
   * 返回（Kmer, 下标位置）
   *
   */
  def indexOne(k: Int, step: Int, ref: String): IndexedSeq[(String, Int)] = {

    //var acc = 0
    //var singleIndex = ArrayBuffer.fill((ref.length - k) / step + 1)(("-1", -1))
    //List[(Int, (String, Int))]()
    /**
     * 用长度为binaryK的滑动窗口按binaryStep的步长遍历整条二进制reference，
     * 遍历次数为len - binaryK + 1次(0 to len - binaryK)
     */
    //      for (i <- 0 to ref.length - k by step) {
    //        val x = ref.substring(i, i + k)
    //        singleIndex(acc) = (x, i)
    //        acc = acc + 1
    //      }

    val result: IndexedSeq[(String, Int)] = for {
      i <- 0 until ref.length - k + 1 by step
    } yield {
      val x = ref.substring(i, i + k).toUpperCase.replace('N', 'A')
      (x, i)
    }

    var last = ("", 0)

    if ((ref.length - k) % step != 0) {
      val i = ref.length - k
      val x = ref.substring(i, i + k).toUpperCase.replace('N', 'A')
      last = (x, i)
    }

    val fin = result :+ last

    if (last._1 == "") result else fin
  }

  def CreateFourmers(iseqquery: IndexedSeq[Int]): WorkingPara = {
    //val j, sk, mm
    var c22 = 0
    val lencc = iseqquery.length - 3
    val wp = new WorkingPara
    wp.init()

    //    // Get 4-mers value of query
    //    for (sk <- 0 until NAA4)
    //    {
    //      wp.taap(sk) = 0
    //    }

    for (j <- 0 until lencc) {
      c22 = iseqquery(j) * NAA3 + iseqquery(j + 1) * NAA2 + iseqquery(j + 2) * NAA1 + iseqquery(j + 3)
      //    if (c22 == 210) {
      //      cout << int(iseqquery[j]) << " " << int(iseqquery[j + 1]) << " " << int(iseqquery[j + 2]) << " " << int(iseqquery[j + 3]) << endl;
      //    }
      //    cout << "c22 = " << c22 << endl;
      wp.taap(c22) += 1
    }

    // Make index
    var mm = 0
    for (sk <- 0 until NAA4) {
      wp.aap_begin(sk) = mm
      mm += wp.taap(sk)
      wp.taap(sk) = 0
    }

    for (j <- 0 until lencc) {
      c22 = iseqquery(j) * NAA3 + iseqquery(j + 1) * NAA2 + iseqquery(j + 2) * NAA1 + iseqquery(j + 3)
      wp.aap_list(wp.aap_begin(c22) + wp.taap(c22)) = j
      wp.taap(c22) += 1
    }

    wp
  }

  def diag_test_aapn_est_circular(iseq2: IndexedSeq[Int], readLen: Int, wp: WorkingPara): (Int, Int, Int) =
    {
      val len1 = readLen
      val len2 = iseq2.length
      val nall = len1 + len2 - 1

      var pp = 0

      for (i <- 0 until nall) wp.diag_score(i) = 0

      val len22 = len2 - 3
      var i1 = len1 - 1

      for (i <- 0 until len22) {
        val c22 = iseq2(i + 0) * NAA3 + iseq2(i + 1) * NAA2 + iseq2(i + 2) * NAA1 + iseq2(i + 3)

        breakable {
          val j = wp.taap(c22)
          if (j == 0) break()
          pp = wp.aap_list(wp.aap_begin(c22))
          for (i <- 1 to j) {
            wp.diag_score(i1 - pp) += 1
            pp = wp.aap_list(wp.aap_begin(c22) + i)
          }
        }
        i1 += 1
      }

      // find the best band range
      val required_aa1 = BEST_NAS
      val band_b = if (required_aa1 >= 1) required_aa1 - 1 else 0
      val band_e = nall - required_aa1
      val band_m = if (band_b + band_width - 1 < band_e) band_b + band_width - 1 else band_e

      var best_score = 0

      for (i <- band_b to band_m) {
        best_score += wp.diag_score(i)
      }

      var from = band_b
      var end = band_m
      var score = best_score
      var k = from

      for (j <- band_m + 1 until band_e) {
        score -= wp.diag_score(k)
        score += wp.diag_score(j)

        k += 1

        if (score > best_score) {
          from = k
          end = j
          best_score = score
        }
      }
      breakable {
        for (j <- from to end) {
          if (wp.diag_score(j) < 5) {
            best_score -= wp.diag_score(j)
            from += 1
          } else break()
        }
      }

      breakable {
        for (j <- end to from by -1) {
          if (wp.diag_score(j) < 5) {
            best_score -= wp.diag_score(j)
            end -= 1
          } else break()
        }
      }

      val band_left = from - len1 + 1
      val band_right = end - len1 + 1

      val best_sum = best_score

      (band_left, band_right, best_sum)
    }

  def local_band_align2(iseq1: IndexedSeq[Int], iseq2: IndexedSeq[Int], band_left: Int, band_right: Int): (Array[Int], Int, Int, Int) =
    {
      val len1 = iseq1.length
      val len2 = iseq2.length
      var best_score = 0
      var talign_info = new Array[Int](5)
      var iden_no = 0
      var alnln = 0
      val bandwidth = band_right - band_left + 1

      //val MaxPartLen = 2 * MAX_READ + 12 * 3

      //iseq1: binary read sequence, len1: reads length
      //iseq2: ref seq segment, len2: ref seq length
      val score_mat = Array.ofDim[Int](len1, bandwidth)
      val iden_mat = Array.ofDim[Int](len1, bandwidth)
      val from1_mat = Array.ofDim[Int](len1, bandwidth)
      val from2_mat = Array.ofDim[Int](len1, bandwidth)
      val alnln_mat = Array.ofDim[Int](len1, bandwidth)
      val gap_array = new Array[Int](MAX_GAP)
      val matrix = Array.ofDim[Int](MAX_AA, MAX_AA)

      val gap = -6
      val ext_gap = -1

      //      val gap_array = for{
      //        i <- 0 until MAX_GAP
      //      } yield {
      //        gap + i * ext_gap
      //      }

      for (i <- 0 until MAX_GAP) {
        gap_array(i) = gap + i * ext_gap
      }

      var k = 0

      for (i <- 0 until MAX_NA) //6
      {
        for (j <- 0 to i) {
          matrix(i)(j) = BLOSUM62_na(k)
          matrix(j)(i) = matrix(i)(j)
          k += 1
        }
      }

      //    int &from1(talign_info(1));
      //    int &from2(talign_info(3));
      //
      //    int &end1(talign_info(2));
      //    int &end2(talign_info(4));
      //
      //    int i, j, k, j1;
      //    int jj, kk;
      //    int best_score1, iden_no1;
      //    int best_from1, best_from2, best_alnln;

      var best_score1 = 0
      var iden_no1 = 0
      var best_from1 = 0
      var best_from2 = 0
      var best_alnln = 0

      iden_no = 0

      talign_info(1) = 0
      talign_info(3) = 0

      if ((band_right >= len2) || (band_left <= -len1) || (band_left > band_right)) return (talign_info, iden_no, alnln, best_score)

      //val bandwidth = band_right - band_left + 1

      for (i <- 0 until len1) {
        for (j1 <- 0 until bandwidth) {
          score_mat(i)(j1) = 0
        } //here index j1 refer to band column
      }

      best_score = 0

      if (band_left < 0) {
        //set score to left border of the matrix within band
        val tband = if (band_right < 0) band_right else 0

        for (k <- band_left to tband) {
          val i = -k
          val j1 = k - band_left
          score_mat(i)(j1) = matrix(iseq1(i))(iseq2(0))

          if (score_mat(i)(j1) > best_score) {
            best_score = score_mat(i)(j1)
            talign_info(1) = i
            talign_info(3) = 0
            talign_info(2) = i
            talign_info(4) = 0
            alnln = 1
          }

          iden_mat(i)(j1) = if (iseq1(i) == iseq2(0)) 1 else 0
          from1_mat(i)(j1) = i
          from2_mat(i)(j1) = 0
          alnln_mat(i)(j1) = 1
        }
      }

      if (band_right >= 0) { //set score to top border of the matrix within band

        val tband = if (band_left > 0) band_left else 0
        val i = 0

        for (j <- tband to band_right) {
          val j1 = j - band_left
          score_mat(i)(j1) = matrix(iseq1(i))(iseq2(j))
          if (score_mat(i)(j1) > best_score) {
            best_score = score_mat(i)(j1)
            talign_info(1) = i
            talign_info(3) = j
            talign_info(2) = i
            talign_info(4) = j
            alnln = 0
          }

          iden_mat(i)(j1) = if (iseq1(i) == iseq2(j)) 1 else 0
          from1_mat(i)(j1) = i
          from2_mat(i)(j1) = j
          alnln_mat(i)(j1) = 1
        }
      }

      for (i <- 1 until len1) {
        for (j1 <- 0 until bandwidth) {
          val j = j1 + i + band_left

          breakable {
            if (j < 1 || j >= len2) break()

            // from (i-1,j-1)
            best_score1 = score_mat(i - 1)(j1)
            if (best_score1 > 0) {
              iden_no1 = iden_mat(i - 1)(j1)

              best_from1 = from1_mat(i - 1)(j1)
              best_from2 = from2_mat(i - 1)(j1)

              best_alnln = alnln_mat(i - 1)(j1) + 1
            } else {
              best_score1 = 0
              iden_no1 = 0

              best_from1 = i
              best_from2 = j

              best_alnln = 1
            }
            // from last row
            var s1 = 0
            var k0 = if (-band_left + 1 - i > 0) -band_left + 1 - i else 0

            var kk = 0
            for (k <- j1 - 1 to k0 by -1) {
              s1 = score_mat(i - 1)(k) + gap_array(kk)
              if (s1 > best_score1) {
                best_score1 = s1
                iden_no1 = iden_mat(i - 1)(k)
                best_from1 = from1_mat(i - 1)(k)
                best_from2 = from2_mat(i - 1)(k)
                best_alnln = alnln_mat(i - 1)(k) + kk + 2
              }
              kk += 1
            }

            k0 = if (j - band_right - 1 > 0) j - band_right - 1 else 0

            kk = 0
            var jj = j1 + 1
            for (k <- i - 2 to k0 by -1) {
              s1 = score_mat(k)(jj) + gap_array(kk)
              if (s1 > best_score1) {
                best_score1 = s1
                iden_no1 = iden_mat(k)(jj)
                best_from1 = from1_mat(k)(jj)
                best_from2 = from2_mat(k)(jj)
                best_alnln = alnln_mat(k)(jj) + kk + 2
              }
              kk += 1
              jj += 1
            }

            best_score1 += matrix(iseq1(i))(iseq2(j))

            if (iseq1(i) == iseq2(j)) iden_no1 += 1

            score_mat(i)(j1) = best_score1
            iden_mat(i)(j1) = iden_no1
            from1_mat(i)(j1) = best_from1
            from2_mat(i)(j1) = best_from2
            alnln_mat(i)(j1) = best_alnln

            if (best_score1 > best_score) {
              best_score = best_score1
              iden_no = iden_no1

              talign_info(2) = i
              talign_info(4) = j

              talign_info(1) = best_from1
              talign_info(3) = best_from2

              alnln = best_alnln
            }
          }

        } //END for j1
      } //END for (i=1; i<len1; i++)

      //printscorematrix(len1,bandwidth);

      (talign_info, iden_no, alnln, best_score)

    } //END int local_band_align2

  def rawScoreToExpect(raw: Int, k: Double, lambda: Double, m: Int, n: Long): Double = {
    k * m * n * exp(-1 * lambda * raw)
  }

  def effectiveLengthSeq(m: Int, hsp: Int, k: Double): Int =
    {
      if ((m - hsp) < 1 / k) (1 / k).toInt else m - hsp
    }

  def expectedHSPlength(k: Double, m: Int, n: Int, h: Double): Int =
    {
      (scala.math.log(k * m * n) / h).toInt
    }

  def effectiveLengthDb(n: Long, hsp: Int, seqnum: Int, k: Double): Long =
    {
      val n_prime = n - (seqnum * hsp)
      if (n_prime < 1 / k) (1 / k).toLong else n_prime
    }

}
