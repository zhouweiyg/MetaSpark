/*
 * Reference.scala for MetaSpark
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

//import java.lang.ref.Reference

/**
 * Created by spark on 14-11-1.
 */

/**
 * Reference类
 * @param Name    reference的名称：e.g. ">Bacteroides_fragilis_NCTC_9434"
 * @param Length  reference的长度：记录一条reference基因序列字符串的总长
 */
case class Reference (var No: Int, Name: String = "" , Ref: String = "", Length: Int = 0){


  def getName = Name

  def getLen = Length

  def getRef = Ref

  def getNo = No

  /**
   * 获取流水号(Id)
   */
  override def toString =
    "(" + "Name: " + Name + ", Length: " + Length + ", Ref75: " + Ref.substring(0,75) + ")"
}
