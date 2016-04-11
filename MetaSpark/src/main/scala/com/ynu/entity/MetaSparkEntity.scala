/*
 * MetaSparkEntity.scala for MetaSpark
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
 
package com.ynu.entity

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

/**
 * MetaSpark运行结果
 * Created by hadoop
 */
case class MetaSparkEntity(ReadName: Int, ReadLength: String, E_Value: String, AlignmentLength: Int, 
                  ReadBegin: Int, ReadEnd: Int,        Strand: String,  Identity: String, 
                  ReferenceSequenceName: String,     RefBegin: Int,   RefEnd: Int) {

  override def equals(that: Any): Boolean = {
    def strictEquals(other: MetaSparkEntity) = 
      this.ReadName.equals(other.ReadName) && this.RefBegin == other.RefBegin && this.RefEnd == other.ReadEnd

    that match {
      case a: AnyRef if this eq a => true
      case p: MetaSparkEntity => strictEquals(p)
      case _ => false
    }
  }

  /**
   * 格式化结果输出
   * @return
   */
  override def toString = {
    // 格式化Identity，保留两位小数，不足两位末位补零，例如94.92 | 100.00
    val twoDecimalPointsIdentity = BigDecimal(Identity).setScale(2, RoundingMode.HALF_UP).toString
    val formatIdentity =  if (twoDecimalPointsIdentity.split('.')(1).length < 2) twoDecimalPointsIdentity + "0" else twoDecimalPointsIdentity

    // 格式化EVale，保留两位有效数字,如果科学计数发的指数< 10，前用0补，例如1.8e-09 | 2.8e-19
    val eValeBigDecimal = BigDecimal(E_Value)
    //val reservedDigit = if (eValeBigDecimal.scale - eValeBigDecimal.precision + 1 < 10) eValeBigDecimal.scale - eValeBigDecimal.precision + 3 else eValeBigDecimal.scale - eValeBigDecimal.precision + 2
    val reservedDigit = eValeBigDecimal.scale - eValeBigDecimal.precision + 2
    val twoDecimalPointsEValue = eValeBigDecimal.setScale(reservedDigit, RoundingMode.HALF_UP).toString();
    val formatEValue = if (eValeBigDecimal.scale - eValeBigDecimal.precision + 1 < 10) {
      val numberSplit = twoDecimalPointsEValue.split('-')
      if (numberSplit.length > 1) {
        // 可以表示成科学计数法
        (if (numberSplit(0).length > 3) numberSplit(0).substring(0, 3) else numberSplit(0)) + "e-0" +  numberSplit(1)
      } else {
        // 只能转化成正常小数形式，例如1.4284701451939707E-6 -> 0.0000014 -> 1.4e-06
        val pointSplit = twoDecimalPointsEValue.split('.')
        val secondStringLen = pointSplit(1).length // 7
        val validNumber = pointSplit(1).toInt.toString // 14
        val validNumberLen = validNumber.length // 2
        (if (validNumberLen > 1) validNumber(0) + "." + validNumber.substring(1) else validNumber(0) + ".0") + "e-0" + (secondStringLen - validNumberLen + 1)
      }

    } else {
      val numberSplit = twoDecimalPointsEValue.split('-')
      (if (numberSplit(0).length > 3) numberSplit(0).substring(0, 3) else numberSplit(0)) + "e-" + (if (numberSplit.length > 1) numberSplit(1) else 0)
    }

    ReadName + "\t" +
      ReadLength + "\t" +
      formatEValue + "\t" +
      //AlignmentLength + "\t" +
      ReadBegin + "\t" +
      ReadEnd + "\t" +
      Strand + "\t" +
      formatIdentity + "%" + "\t" +
      RefBegin + "\t" +
      RefEnd + "\t" +
      ReferenceSequenceName

    //        ReadName + "\t" +
    //          ReadLength + "\t" +
    //          E_Value + "\t" +
    //          //AlignmentLength + "\t" +
    //          ReadBegin + "\t" +
    //          ReadEnd + "\t" +
    //          Strand + "\t" +
    //          Identity + "%" + "\t" +
    //          RefBegin + "\t" +
    //          RefEnd + "\t" +
    //          ReferenceSequenceName
  }

}


