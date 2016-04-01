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
