/*
 * Read.scala for MetaSpark
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

/**
 * Created by spark on 14-12-12.
 * modify liucc 2015-08-01 10:52:05
 *        增加方向信息
 *        +： 正向
 *        -： 反向
 */
class Read(var No: Int, val readName: String = "", val readLen: Int = 0, val readDirection: String = "+") {
  def this(name: String, length: Int, direction: String) = {
    //val arr: Array[String] = name.split("_")
    this(name.split("_")(2).toInt, name, length, direction)
  }
  //def getID() = Read.getID
  override def toString = No + "\t" + readName + "\t" + readLen + "\t" + readDirection + "\t"
}

//object Read {
//
//  private val Id = new AtomicInteger()
//
//  def getID = Id.incrementAndGet()
//
//}

