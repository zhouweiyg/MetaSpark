
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

